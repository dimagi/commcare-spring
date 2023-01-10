package org.commcare.formplayer.services

import com.google.common.collect.ImmutableList
import java.io.InputStream
import java.net.URLConnection

/**
 * Utility functions to validate media attachments in a form
 */
object MediaValidator {

    // 3 MB size limit
    private const val MAX_BYTES_PER_ATTACHMENT = (3 * 1048576 - 1024).toLong()
    private val SUPPORTED_FILE_EXTS = ImmutableList.of(".jpg", "jpeg", "png", "pdf")
    private val SUPPORTED_MIME_TYPES = ImmutableList.of("image", "application/pdf")

    @JvmStatic
    fun isFileTooLarge(size: Long): Boolean {
        return size > MAX_BYTES_PER_ATTACHMENT
    }

    @JvmStatic
    fun isUnSupportedFileExtension(fileName: String): Boolean {
        return SUPPORTED_FILE_EXTS.find { fileName.endsWith(it, true) }
            .isNullOrBlank()
    }

    @JvmStatic
    fun isUnsupportedMimeType(fis: InputStream, fileName: String): Boolean {
        var mimeType: String? = URLConnection.guessContentTypeFromStream(fis)
        if (mimeType.isNullOrBlank()) {
            mimeType = URLConnection.guessContentTypeFromName(fileName)
        }
        return mimeType.isNullOrBlank() ||
            SUPPORTED_MIME_TYPES.find { mimeType.startsWith(it) }.isNullOrBlank()
    }
}