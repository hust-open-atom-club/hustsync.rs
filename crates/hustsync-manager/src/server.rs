use std::error::Error;

use hustsync_config_parser::ManagerConfig;

pub struct Manager {
    config: ManagerConfig, // ? restful_server
                           // ? db adapter
                           // ? sync semaphore
                           // ? http client
}

pub fn get_hustsync_manager(config: ManagerConfig) -> Result<Manager, Box<dyn Error>> {
    // ? set manager restful mode ?
    Ok(Manager { config })
}
