const jobArgs = process.argv.slice(2);
const scriptsPath = jobArgs[0];
const job = JSON.parse(jobArgs[1]);
const scriptName = job.class;
const scriptArgs = job.args;
const task = require(`${scriptsPath}/${scriptName}`);
task.perform.apply(this, scriptArgs);
