//! Windows Job Objects, one per job: memory limit, whole-tree kill and limit detection.

use std::{
    collections::HashMap,
    io, mem, ptr,
    sync::{Mutex, PoisonError},
};

use hustsync_config_parser::{MemBytes, WorkerCgroupConfig};
use tokio::process::{Child, Command};
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE},
    System::{
        IO::{CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED},
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_JOB_MEMORY,
            JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_ASSOCIATE_COMPLETION_PORT,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectAssociateCompletionPortInformation,
            JobObjectExtendedLimitInformation, SetInformationJobObject, TerminateJobObject,
        },
        SystemServices::JOB_OBJECT_MSG_JOB_MEMORY_LIMIT,
    },
};

/// Job objects of running jobs, keyed by mirror name.
#[derive(Debug, Default)]
pub struct Cgroups {
    jobs: Mutex<HashMap<String, Job>>,
}

impl Cgroups {
    pub fn init(_cfg: &WorkerCgroupConfig) -> io::Result<Self> {
        Ok(Self::default())
    }

    /// Creates `name`'s job; the child joins it in `adopt`, right after spawning.
    #[expect(clippy::unused_async, reason = "same signature as the Linux version")]
    pub async fn enter(
        &self,
        name: &str,
        mem: Option<MemBytes>,
        _cmd: &mut Command,
    ) -> io::Result<()> {
        let job = Job::new(mem)?;
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(name.to_owned(), job);
        Ok(())
    }

    /// Puts the spawned child in `name`'s job; everything it starts afterwards follows.
    pub fn adopt(&self, name: &str, child: &Child) -> io::Result<()> {
        let (Some(job), Some(proc)) = (self.handle(name), child.raw_handle()) else {
            return Ok(());
        };
        // SAFETY: both handles stay open for the duration of the call.
        check(unsafe { AssignProcessToJobObject(job, proc) })
    }

    /// Ends every process in `name`'s job at once.
    pub fn kill(&self, name: &str) {
        if let Some(job) = self.handle(name) {
            // SAFETY: the job handle is open until `finish` drops it.
            unsafe { TerminateJobObject(job, 1) };
        }
    }

    /// Drops `name`'s job, killing leftovers; returns whether it hit its memory limit.
    #[expect(clippy::unused_async, reason = "same signature as the Linux version")]
    pub async fn finish(&self, name: &str) -> bool {
        let job = self
            .jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(name);
        job.is_some_and(|j| j.hit_limit())
    }

    fn handle(&self, name: &str) -> Option<HANDLE> {
        self.jobs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(name)
            .map(|j| j.job)
    }
}

/// A job object and the completion port it reports limit hits to.
#[derive(Debug)]
struct Job {
    job: HANDLE,
    port: HANDLE,
}

// SAFETY: kernel handles are usable from any thread.
unsafe impl Send for Job {}

impl Job {
    fn new(mem: Option<MemBytes>) -> io::Result<Self> {
        // SAFETY: plain Win32 calls; every handle is owned by `Job` and closed on drop.
        unsafe {
            let job = CreateJobObjectW(ptr::null(), ptr::null());
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            let port = CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, 1);
            if port.is_null() {
                let err = io::Error::last_os_error();
                CloseHandle(job);
                return Err(err);
            }
            let this = Self { job, port };

            let mut limit: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = mem::zeroed();
            limit.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if let Some(m) = mem {
                limit.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_JOB_MEMORY;
                limit.JobMemoryLimit = usize::try_from(m.0).unwrap_or(usize::MAX);
            }
            check(SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                (&raw const limit).cast(),
                size_of_u32::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>(),
            ))?;

            let assoc = JOBOBJECT_ASSOCIATE_COMPLETION_PORT {
                CompletionKey: ptr::null_mut(),
                CompletionPort: port,
            };
            check(SetInformationJobObject(
                job,
                JobObjectAssociateCompletionPortInformation,
                (&raw const assoc).cast(),
                size_of_u32::<JOBOBJECT_ASSOCIATE_COMPLETION_PORT>(),
            ))?;
            Ok(this)
        }
    }

    /// Whether the job reported hitting its memory limit.
    fn hit_limit(&self) -> bool {
        let (mut msg, mut key, mut ov) = (0u32, 0usize, ptr::null_mut::<OVERLAPPED>());
        // SAFETY: the port is open; a zero timeout only drains messages already queued.
        while unsafe { GetQueuedCompletionStatus(self.port, &mut msg, &mut key, &mut ov, 0) } != 0 {
            if msg == JOB_OBJECT_MSG_JOB_MEMORY_LIMIT {
                return true;
            }
        }
        false
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: both handles are owned here and closed exactly once.
        unsafe {
            CloseHandle(self.job);
            CloseHandle(self.port);
        }
    }
}

fn check(ok: i32) -> io::Result<()> {
    if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn size_of_u32<T>() -> u32 {
    u32::try_from(mem::size_of::<T>()).unwrap_or(u32::MAX)
}
