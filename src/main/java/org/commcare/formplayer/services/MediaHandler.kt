package org.commcare.formplayer.services

import org.commcare.formplayer.objects.MediaMetadataRecord
import org.commcare.formplayer.objects.SerializableFormSession
import org.commcare.formplayer.services.MediaValidator.isFileTooLarge
import org.commcare.formplayer.services.MediaValidator.isUnSupportedFileExtension
import org.commcare.formplayer.services.MediaValidator.isUnsupportedMimeType
import org.commcare.util.FileUtils
import org.javarosa.core.services.locale.Localization
import org.javarosa.core.util.PropertyUtils
import org.springframework.web.multipart.MultipartFile
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Supporting methods to process and save media files on the filesystem
 */
class MediaHandler(val file: MultipartFile) {

    /**
     * Saves file in the given parent directory
     */
    fun saveFile(parentDirPath: Path, session: SerializableFormSession, username: String, asUser: String?,
                 domain: String, appId: String): String {
        validateFile()
        val fileId = PropertyUtils.genUUID()
        val parent = parentDirPath.toFile()
        parent.mkdirs()
        var fileIdWithExt = fileId
        val fileExtension = FileUtils.getExtension(file.originalFilename)
        fileExtension?.let { fileIdWithExt = "$fileId.$it" }
        val filePath = getMediaFilePath(parentDirPath, fileIdWithExt)
        val destinationFile = filePath.toFile()


        try {
            FileUtils.copyFile(file.inputStream, destinationFile)
        } catch (e: IOException) {
            throw IOException("Could not copy file to destination due to " + e.message, e)
        }
        val mediaMetaDataService = MediaMetaDataService()
        val mediaMetaData = MediaMetadataRecord(
            fileId,
            filePath.toString(),
            session,
            fileExtension,
            file.size.toInt(),
            username,
            asUser,
            domain,
            appId
        )
        mediaMetaDataService.saveMediaMetaData(mediaMetaData)

        return fileIdWithExt
    }

    private fun validateFile() {
        if (isUnSupportedFileExtension(file.originalFilename) && isUnsupportedMimeType(
                file.inputStream,
                file.name
            )
        ) {
            val unsupportedFileExtError = Localization.get("form.attachment.invalid")
            throw RuntimeException(unsupportedFileExtError)
        } else if (isFileTooLarge(file.size)) {
            val fileOversizeError = Localization.get("file.oversize.error.message")
            throw RuntimeException(fileOversizeError)
        }
    }

    fun getMediaFilePath(parentDirPath: Path, fileId: String): Path {
        return Paths.get(parentDirPath.toString(), fileId)
    }

    fun cleanMedia(parentDirPath: Path, fileId: String): Boolean {
        try {
            val mediaMetaDataService = MediaMetaDataService()
            mediaMetaDataService.deleteMetaDataById(fileId)
        } catch (e: Exception) {
            // ignore, we don't want to crash even if delete fails
        }
        val currentMedia = getMediaFilePath(parentDirPath, fileId).toFile()
        return currentMedia.delete()
    }
}