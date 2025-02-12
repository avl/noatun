use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::{MessageId, NoatunTime};
use anyhow::Result;
use tracing::{info, warn};

pub(crate) struct UpdateHeadTracker {
    file: FileAccessor,
}

impl UpdateHeadTracker {
    pub(crate) fn remove_update_head(&mut self, message_id: MessageId) -> Result<()> {
        let mapping = self.file.map_mut();
        let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);
        if let Some(index) = id_mapping.iter().position(|id| *id == message_id) {
            if index + 1 < id_mapping.len() {
                id_mapping.swap(index, id_mapping.len()-1);
            }
            let new_len = id_mapping.len() - 1; //len can't be 0, since then we couldn't have found 'message_id' in id_mapping
            self.file.fast_truncate(new_len*size_of::<MessageId>());
        }
        Ok(())
    }
    pub(crate) fn remove_before_cutoff(&mut self, cutoff: NoatunTime) -> Result<()> {
        info!("Deleting all heads before cutoff: {:?}", cutoff);
        let mapping = self.file.map_mut();
        let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);
        let mut index = 0;
        let mut mapping_len = id_mapping.len();
        while index < mapping_len {
            if id_mapping[index].timestamp() < cutoff {
                info!("Deleting all head: {:?}", id_mapping[index]);
                id_mapping.swap(index, id_mapping.len()-1);
                mapping_len -= 1;
            } else {
                index += 1;
            }
        }
        self.file.fast_truncate(mapping_len*size_of::<MessageId>());

        Ok(())

    }

    pub(crate) fn add_new_update_head(
        &mut self,
        new_message_id: MessageId,
        subsumed: &[MessageId],
        cutoff: NoatunTime,
    ) -> anyhow::Result<()> {
        if new_message_id.timestamp() < cutoff {
            warn!("Not adding update-head {:?} because cutoff {:?}", new_message_id, cutoff);  //TODO: not warn!
            return Ok(());
        }
        info!("Adding update-head {:?} (cutoff {:?})", new_message_id, cutoff);  //TODO: not warn!
        let mapping = self.file.map_mut();
        let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);
        let mut i = 0;
        let mut file_len = id_mapping.len();
        let mut maplen = id_mapping.len();
        while i < maplen {
            if subsumed.contains(&id_mapping[i]) {
                if i != maplen - 1 {
                    id_mapping.swap(i, maplen - 1);
                }
                maplen -= 1;
            } else {
                i += 1;
            }
        }
        if maplen == file_len {
            self.file.grow((file_len + 1) * size_of::<MessageId>())?;
            file_len += 1;
        }

        let mapping = self.file.map_mut();
        let id_mapping: &mut [MessageId] = bytemuck::cast_slice_mut(mapping);

        id_mapping[maplen] = new_message_id;
        maplen += 1;

        if maplen < file_len {
            self.file.fast_truncate(maplen * size_of::<MessageId>());
        }
        Ok(())
    }
    pub(crate) fn clear(&mut self) {
        self.file.fast_truncate(0);
    }

    pub(crate) fn get_update_heads(&self) -> &[MessageId] {
        bytemuck::cast_slice(self.file.map())
    }
    pub(crate) fn new<D: Disk>(disk: &mut D, target: &crate::Target) -> Result<UpdateHeadTracker> {
        Ok(Self {
            file: disk.open_file(target, "update_head", 0, 10 * 1024 * 1024)?,
        })
    }
}
