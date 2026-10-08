use crate::{Error, Result};
use tokio::process::{Child, Command};

pub fn configure(command: &mut Command) {
    #[cfg(unix)]
    command.process_group(0);
    #[cfg(windows)]
    command.creation_flags(0x08000000); // CREATE_NO_WINDOW
    let _ = command;
}
#[cfg(unix)]
pub struct Tree(u32);
#[cfg(unix)]
impl Tree {
    pub fn attach(child: &Child) -> Result<Self> {
        Ok(Self(child.id().ok_or(Error::Unavailable)?))
    }
}
#[cfg(unix)]
impl Drop for Tree {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.0 as i32), libc::SIGKILL);
        }
    }
}

#[cfg(windows)]
pub struct Tree(windows_sys::Win32::Foundation::HANDLE);
#[cfg(windows)]
unsafe impl Send for Tree {}
#[cfg(windows)]
unsafe impl Sync for Tree {}
#[cfg(windows)]
impl Tree {
    pub fn attach(child: &Child) -> Result<Self> {
        use windows_sys::Win32::{
            Foundation::*,
            System::{JobObjects::*, Threading::*},
        };
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job.is_null() {
                return Err(Error::Unavailable);
            }
            let tree = Self(job);
            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            if SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as _,
                std::mem::size_of_val(&info) as u32,
            ) == 0
            {
                return Err(Error::Unavailable);
            }
            let process = OpenProcess(
                PROCESS_SET_QUOTA | PROCESS_TERMINATE,
                false as _,
                child.id().ok_or(Error::Unavailable)?,
            );
            if process.is_null() {
                return Err(Error::Unavailable);
            }
            let assigned = AssignProcessToJobObject(job, process);
            CloseHandle(process);
            if assigned == 0 {
                return Err(Error::Unavailable);
            }
            Ok(tree)
        }
    }
}
#[cfg(windows)]
impl Drop for Tree {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

impl Tree {
    pub fn terminate(&self) {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(self.0 as i32), libc::SIGKILL);
        }
        #[cfg(windows)]
        unsafe {
            windows_sys::Win32::System::JobObjects::TerminateJobObject(self.0, 1);
        }
    }
}
pub struct Cancellation<'a> {
    pub tree: &'a Tree,
    pub armed: bool,
}
impl Drop for Cancellation<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.tree.terminate();
        }
    }
}
