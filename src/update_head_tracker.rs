use crate::disk_abstraction::Disk;
use crate::disk_access::FileAccessor;
use crate::MessageId;
use anyhow::Result;

pub(crate) struct UpdateHeadTracker {
    file: FileAccessor,
}

impl UpdateHeadTracker {
    pub(crate) fn add_new_update_head(
        &mut self,
        new_message_id: MessageId,
        subsumed: &[MessageId],
    ) -> anyhow::Result<()> {
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
