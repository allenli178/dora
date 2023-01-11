use eyre::{eyre, Context};
use raw_sync::events::{Event, EventImpl, EventInit, EventState};
use serde::{Deserialize, Serialize};
use shared_memory::Shmem;
use std::{
    mem, slice,
    sync::atomic::{AtomicBool, AtomicU64},
    time::Duration,
};

pub struct ShmemChannel {
    memory: Shmem,
    server_event: Box<dyn EventImpl>,
    client_event: Box<dyn EventImpl>,
    disconnect_offset: usize,
    len_offset: usize,
    data_offset: usize,
    server: bool,
}

#[allow(clippy::missing_safety_doc)]
impl ShmemChannel {
    pub unsafe fn new_server(memory: Shmem) -> eyre::Result<Self> {
        let (server_event, server_event_len) = unsafe { Event::new(memory.as_ptr(), true) }
            .map_err(|err| eyre!("failed to open raw server event: {err}"))?;
        let (client_event, client_event_len) =
            unsafe { Event::new(memory.as_ptr().wrapping_add(server_event_len), true) }
                .map_err(|err| eyre!("failed to open raw client event: {err}"))?;
        let (disconnect_offset, len_offset, data_offset) =
            offsets(server_event_len, client_event_len);

        server_event
            .set(EventState::Clear)
            .map_err(|err| eyre!("failed to init server_event: {err}"))?;
        client_event
            .set(EventState::Clear)
            .map_err(|err| eyre!("failed to init client_event: {err}"))?;
        unsafe {
            memory
                .as_ptr()
                .wrapping_add(disconnect_offset)
                .cast::<AtomicBool>()
                .write(AtomicBool::new(false));
        }
        unsafe {
            memory
                .as_ptr()
                .wrapping_add(len_offset)
                .cast::<AtomicU64>()
                .write(AtomicU64::new(0));
        }

        Ok(Self {
            memory,
            server_event,
            client_event,
            disconnect_offset,
            len_offset,
            data_offset,
            server: true,
        })
    }

    pub unsafe fn new_client(memory: Shmem) -> eyre::Result<Self> {
        let (server_event, server_event_len) = unsafe { Event::from_existing(memory.as_ptr()) }
            .map_err(|err| eyre!("failed to open raw server event: {err}"))?;
        let (client_event, client_event_len) =
            unsafe { Event::from_existing(memory.as_ptr().wrapping_add(server_event_len)) }
                .map_err(|err| eyre!("failed to open raw client event: {err}"))?;
        let (disconnect_offset, len_offset, data_offset) =
            offsets(server_event_len, client_event_len);

        Ok(Self {
            memory,
            server_event,
            client_event,
            disconnect_offset,
            len_offset,
            data_offset,
            server: false,
        })
    }

    pub fn send<T>(&mut self, value: &T) -> eyre::Result<()>
    where
        T: Serialize + std::fmt::Debug,
    {
        let msg = bincode::serialize(value).wrap_err("failed to serialize value")?;

        self.send_raw(&msg)
    }

    fn send_raw(&mut self, msg: &[u8]) -> Result<(), eyre::ErrReport> {
        assert!(msg.len() <= self.memory.len() - self.data_offset);
        // write data first
        unsafe {
            self.data_mut()
                .copy_from_nonoverlapping(msg.as_ptr(), msg.len());
        }
        // write len second for synchronization
        self.data_len()
            .store(msg.len() as u64, std::sync::atomic::Ordering::Release);

        // signal event
        let event = if self.server {
            &self.client_event
        } else {
            &self.server_event
        };
        event
            .set(EventState::Signaled)
            .map_err(|err| eyre!("failed to send message over ShmemChannel: {err}"))?;
        Ok(())
    }

    pub fn receive<T>(&mut self, timeout: Option<Duration>) -> eyre::Result<Option<T>>
    where
        T: for<'a> Deserialize<'a> + std::fmt::Debug,
    {
        // wait for event
        let event = if self.server {
            &self.server_event
        } else {
            &self.client_event
        };
        let timeout = timeout
            .map(raw_sync::Timeout::Val)
            .unwrap_or(raw_sync::Timeout::Infinite);
        event
            .wait(timeout)
            .map_err(|err| eyre!("failed to receive from ShmemChannel: {err}"))?;

        // check for disconnect first
        if self.disconnect().load(std::sync::atomic::Ordering::Acquire) {
            if self.server {
                tracing::trace!("shm client disconnected");
            } else {
                tracing::error!("shm server disconnected");
            }
            return Ok(None);
        }

        // then read len for synchronization
        let msg_len = self.data_len().load(std::sync::atomic::Ordering::Acquire) as usize;
        assert_ne!(msg_len, 0);
        assert!(msg_len < self.memory.len() - self.data_offset);

        // finally read the data
        let value_raw = unsafe { slice::from_raw_parts(self.data(), msg_len) };

        bincode::deserialize(value_raw)
            .wrap_err("failed to deserialize value")
            .map(|v| Some(v))
    }

    fn disconnect(&self) -> &AtomicBool {
        unsafe {
            &*self
                .memory
                .as_ptr()
                .wrapping_add(self.disconnect_offset)
                .cast::<AtomicBool>()
        }
    }

    fn data_len(&self) -> &AtomicU64 {
        unsafe {
            &*self
                .memory
                .as_ptr()
                .wrapping_add(self.len_offset)
                .cast::<AtomicU64>()
        }
    }

    fn data(&self) -> *const u8 {
        self.memory.as_ptr().wrapping_add(self.data_offset)
    }

    fn data_mut(&mut self) -> *mut u8 {
        self.memory.as_ptr().wrapping_add(self.data_offset)
    }
}

fn offsets(server_event_len: usize, client_event_len: usize) -> (usize, usize, usize) {
    let disconnect_offset = server_event_len + client_event_len;
    let len_offset = disconnect_offset + mem::size_of::<AtomicBool>();
    let data_offset = len_offset + mem::size_of::<AtomicU64>();
    (disconnect_offset, len_offset, data_offset)
}

unsafe impl Send for ShmemChannel {}

impl Drop for ShmemChannel {
    fn drop(&mut self) {
        if self.server {
            // server must only exit after client is disconnected
            let disconnected = self.disconnect().load(std::sync::atomic::Ordering::Acquire);
            if disconnected {
                tracing::debug!("closing ShmemServer after client disconnect");
            } else {
                tracing::error!("ShmemServer closed before client disconnect");
            }
        } else {
            tracing::debug!("disconnecting client");

            self.disconnect()
                .store(true, std::sync::atomic::Ordering::Release);

            // wake up server
            if let Err(err) = self.server_event.set(EventState::Signaled) {
                tracing::warn!("failed to signal ShmemChannel disconnect: {err}");
            }
        }
    }
}