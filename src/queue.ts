export function createQueue() {
  let tail = Promise.resolve();
  return (job: () => Promise<void>) => {
    const run = tail.then(job, job);
    tail = run.then(
      () => {},
      () => {},
    );
    return run;
  };
}
