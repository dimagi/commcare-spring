package org.commcare.formplayer.services;

import org.commcare.formplayer.objects.MediaMetadataRecord;
import org.commcare.formplayer.repo.MediaMetaDataRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;


/**
 * Service for managing media metadata
 */
@Service
public class MediaMetaDataService {

    @Autowired
    private MediaMetaDataRepo mediaMetaDataRepo;

    public int purge() {
        return mediaMetaDataRepo.deleteMetaDataWithoutFormSessionId();
    }

    public MediaMetadataRecord findByFilePath(String filePath) {
        return mediaMetaDataRepo.findByFilePath(filePath).get();
    }
    public void saveMediaMetaData(MediaMetadataRecord mediaMetadataRecord) {
        mediaMetaDataRepo.save(mediaMetadataRecord);
    }

    public void deleteMetaDataById(String id) {
        mediaMetaDataRepo.deleteById(id);
    }
}