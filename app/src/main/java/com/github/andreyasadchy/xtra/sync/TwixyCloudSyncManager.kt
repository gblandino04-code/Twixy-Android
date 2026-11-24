package com.github.andreyasadchy.xtra.sync

import android.content.Context
import androidx.core.content.edit
import com.github.andreyasadchy.xtra.model.VideoPosition
import com.github.andreyasadchy.xtra.model.ui.Bookmark
import com.github.andreyasadchy.xtra.repository.BookmarksRepository
import com.github.andreyasadchy.xtra.repository.PlayerRepository
import com.github.andreyasadchy.xtra.util.TwitchApiHelper
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.TimeUnit

class TwixyCloudSyncManager(
    private val appContext: Context,
    private val bookmarksRepository: BookmarksRepository,
    private val playerRepository: PlayerRepository,
    private val okHttpClient: Lazy<OkHttpClient>,
    private val baseUrl: String = DEFAULT_SYNC_URL,
) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val mutex = Mutex()
    private val metaPrefs by lazy { appContext.getSharedPreferences("twixy_cloud_sync_meta", Context.MODE_PRIVATE) }
    private val jsonMedia = "application/json; charset=utf-8".toMediaType()
    @Volatile private var lastPushMs = 0L

    fun markUpdated(videoId: String, tsSec: Double = System.currentTimeMillis() / 1000.0) {
        val k = videoId.trim()
        if (k.isEmpty()) return
        metaPrefs.edit {
            putString("upd_$k", tsSec.toString())
            remove("del_$k")
        }
    }

    fun markDeleted(videoId: String, tsSec: Double = System.currentTimeMillis() / 1000.0) {
        val k = videoId.trim()
        if (k.isEmpty()) return
        metaPrefs.edit {
            putString("del_$k", tsSec.toString())
            remove("upd_$k")
        }
    }

    private fun getUpdatedTs(key: String): Double =
        metaPrefs.getString("upd_$key", null)?.toDoubleOrNull() ?: 0.0

    private fun getDeletedTs(key: String): Double =
        metaPrefs.getString("del_$key", null)?.toDoubleOrNull() ?: 0.0

    fun onLocalBookmarkSaved(videoId: String) {
        markUpdated(videoId)
        syncNowAsync(force = true)
    }

    fun onLocalBookmarkDeleted(videoId: String) {
        markDeleted(videoId)
        syncNowAsync(force = true)
    }

    fun onLocalVodPositionUpdated(
        videoId: String,
        positionMs: Long,
        durationSec: Int?,
        channelId: String?,
        channelLogin: String?,
        channelName: String?,
        channelImage: String?,
        gameName: String?,
        title: String?,
        thumbnail: String?,
        createdAt: String?,
    ) {
        val vid = videoId.trim()
        val posSec = (positionMs / 1000L).toInt().coerceAtLeast(0)
        if (vid.isEmpty() || posSec < 5) return

        scope.launch {
            val existing = bookmarksRepository.getByVideoId(vid)
            if (existing == null && getDeletedTs(vid) <= 0.0) {
                bookmarksRepository.save(
                    Bookmark(
                        videoId = vid,
                        userId = channelId,
                        userLogin = channelLogin,
                        userName = channelName ?: channelLogin,
                        userLogo = channelImage,
                        gameName = gameName ?: "Archivio VOD",
                        title = title ?: "VOD",
                        createdAt = createdAt,
                        thumbnail = thumbnail,
                        type = "archive",
                        duration = (durationSec ?: 0).coerceAtLeast(posSec).toString(),
                    )
                )
            }
            markUpdated(vid)
            syncNow(force = false)
        }
    }

    fun syncNowAsync(force: Boolean = false) {
        scope.launch { syncNow(force) }
    }

    suspend fun syncNow(force: Boolean = false): Boolean {
        if (baseUrl.contains("TUO-SOTTODOMINIO")) return false
        val token = TwitchApiHelper.getAnyTwitchToken(appContext) ?: return false
        val nowMs = System.currentTimeMillis()
        if (!force && (nowMs - lastPushMs) < 12_000L) return false

        return mutex.withLock {
            lastPushMs = System.currentTimeMillis()
            runCatching {
                val bookmarks = bookmarksRepository.getAll()
                val posMap = playerRepository.getAllVideoPositions().associateBy { it.id.toString() }
                val payloadItems = JSONArray()
                val localKeys = HashSet<String>()

                for (bm in bookmarks) {
                    val vid = bm.videoId?.trim().orEmpty()
                    if (vid.isEmpty()) continue
                    localKeys.add(vid)
                    val posSec = ((posMap[vid]?.position ?: 0L) / 1000L).toInt().coerceAtLeast(0)
                    val durSec = bm.duration?.toIntOrNull()?.coerceAtLeast(posSec) ?: posSec
                    val upd = getUpdatedTs(vid).takeIf { it > 0.0 } ?: (System.currentTimeMillis() / 1000.0)
                    payloadItems.put(
                        JSONObject().apply {
                            put("key", vid)
                            put("vod_id", vid)
                            put("channel", (bm.userLogin ?: "").lowercase())
                            put("display_name", bm.userName ?: bm.userLogin ?: "Streamer")
                            put("game", bm.gameName ?: "Archivio VOD")
                            put("title", bm.title ?: "VOD")
                            put("thumbnail_url", bm.thumbnail?.takeIf { it.startsWith("http") } ?: "")
                            put("category_art_url", "")
                            put("avatar_url", bm.userLogo?.takeIf { it.startsWith("http") } ?: "")
                            put("position_sec", posSec)
                            put("duration_sec", durSec)
                            put("bookmarked", true)
                            put("is_live_saved", false)
                            put("updated_at", upd)
                            put("deleted", false)
                        }
                    )
                }

                for ((prefKey, prefVal) in metaPrefs.all) {
                    if (!prefKey.startsWith("del_")) continue
                    val delKey = prefKey.removePrefix("del_")
                    if (delKey in localKeys) continue
                    val ts = (prefVal as? String)?.toDoubleOrNull() ?: continue
                    payloadItems.put(
                        JSONObject().apply {
                            put("key", delKey)
                            put("vod_id", if (delKey.startsWith("live:")) "" else delKey)
                            put("deleted", true)
                            put("updated_at", ts)
                        }
                    )
                }

                val client = okHttpClient.value.newBuilder()
                    .connectTimeout(6, TimeUnit.SECONDS)
                    .readTimeout(8, TimeUnit.SECONDS)
                    .build()

                val req = Request.Builder()
                    .url("${baseUrl.trimEnd('/')}/v1/vods/sync")
                    .header("Authorization", "OAuth $token")
                    .post(JSONObject().put("items", payloadItems).toString().toRequestBody(jsonMedia))
                    .build()

                client.newCall(req).execute().use { resp ->
                    if (!resp.isSuccessful) return@use false
                    val body = resp.body.string()
                    val remoteArr = JSONObject(body).optJSONArray("items") ?: return@use true

                    for (i in 0 until remoteArr.length()) {
                        val o = remoteArr.getJSONObject(i)
                        val key = o.optString("key").trim()
                        val vid = o.optString("vod_id").trim().ifEmpty { key }
                        if (vid.isEmpty() || vid.startsWith("live:")) continue

                        val remUpd = o.optDouble("updated_at", 0.0)
                        val remDel = o.optBoolean("deleted", false)
                        val locUpd = getUpdatedTs(vid)
                        val locDel = getDeletedTs(vid)
                        val existing = bookmarksRepository.getByVideoId(vid)

                        if (remDel) {
                            if (remUpd >= locUpd && existing != null) {
                                bookmarksRepository.delete(existing)
                            }
                            if (remUpd > locDel) {
                                markDeleted(vid, remUpd)
                            }
                        } else {
                            if (locDel > remUpd) continue
                            if (existing == null || remUpd > locUpd + 0.01) {
                                val posSec = o.optInt("position_sec", 0).coerceAtLeast(0)
                                val durSec = o.optInt("duration_sec", 0).coerceAtLeast(posSec)
                                val thumb = o.optString("thumbnail_url").ifBlank { o.optString("category_art_url") }
                                val avatar = o.optString("avatar_url")
                                val channel = o.optString("channel")
                                val disp = o.optString("display_name").ifBlank { channel }

                                if (existing == null) {
                                    bookmarksRepository.save(
                                        Bookmark(
                                            videoId = vid,
                                            userLogin = channel,
                                            userName = disp,
                                            userLogo = avatar.ifBlank { null },
                                            gameName = o.optString("game").ifBlank { "Archivio VOD" },
                                            title = o.optString("title").ifBlank { "VOD" },
                                            thumbnail = thumb.ifBlank { null },
                                            type = "archive",
                                            duration = durSec.toString(),
                                        )
                                    )
                                } else {
                                    bookmarksRepository.update(
                                        existing.apply {
                                            if (disp.isNotBlank()) userName = disp
                                            if (channel.isNotBlank()) userLogin = channel
                                            o.optString("game").takeIf { it.isNotBlank() }?.let { gameName = it }
                                            o.optString("title").takeIf { it.isNotBlank() }?.let { title = it }
                                            if (durSec > 0) duration = durSec.toString()
                                            if (thumbnail.isNullOrBlank() && thumb.isNotBlank()) thumbnail = thumb
                                            if (userLogo.isNullOrBlank() && avatar.isNotBlank()) userLogo = avatar
                                        }
                                    )
                                }

                                vid.toLongOrNull()?.let { vidLong ->
                                    playerRepository.saveVideoPosition(VideoPosition(vidLong, posSec * 1000L))
                                }
                                markUpdated(vid, remUpd)
                            }
                        }
                    }
                    true
                }
            }.getOrDefault(false)
        }
    }

    companion object {
        const val DEFAULT_SYNC_URL = "https://twixy-sync.twixy-sync.workers.dev"
    }
}
