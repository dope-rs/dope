pub mod common;

use dope_core::platform::affinity::Cpu;
use dope_runtime::process::{
    self,
    failure::{bus, execute},
};

use crate::common::process::Cpus as _;

fn main() {
    let [cpu] = <[Cpu; 1]>::try_current().expect("required available CPUs");
    let runtime = process::Layout::pinned([cpu])
        .and_then(|layout| layout.try_map([cpu], |cpu| cpu))
        .expect("one process worker fits");
    let mut storage = std::mem::MaybeUninit::uninit();
    let result = runtime.run(o3::mem::slot::Vacant::new(&mut storage), |cpu, context| {
        assert!(context.cpu() == cpu);
        Err(context.terminal().fail())
    });
    assert!(
        result.is_err(),
        "a reported entry failure must fail the process run"
    );
    if let Err(error) = result {
        let faults = error.fold(Vec::new(), |mut faults, fault| {
            faults.push(fault);
            faults
        });
        assert_eq!(faults.len(), 2);
        assert!(faults.iter().any(|fault| matches!(
            fault,
            execute::Fault::Control(execute::Cause::WorkerFailed { cpu: actual }) if *actual == cpu
        )));
        assert!(faults.iter().any(|fault| matches!(
            fault,
            execute::Fault::Worker(execute::Worker::Entry {
                cpu: actual,
                source: Ok(bus::Event::Failed),
                bus: bus::Report::Clean,
                terminal: None,
                handoff: None,
            }) if *actual == cpu
        )));
    }
}
