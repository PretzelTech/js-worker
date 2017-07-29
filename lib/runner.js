#!/usr/bin/env node
require('dotenv').config()

const path = require('path');
const fork = require('child_process').fork;
const os = require('os');
const redis = require('redis');

const RK_PREFIX = 'resque';

const RK_QUEUE = `${RK_PREFIX}:queue`;
const RK_WORKER = `${RK_PREFIX}:worker`;
const RK_WORKERS = `${RK_PREFIX}:workers`;
const RK_STAT = `${RK_PREFIX}:stat`;
const RK_FAILED = `${RK_PREFIX}:failed`;


const JOB_ERROR_TYPES = ['exception', 'error', 'backtrace'];

const QUEUE = process.env['QUEUE'];
const COUNT = parseInt(process.env['COUNT'], 10);
const INTERVAL = parseFloat(process.env['INTERVAL']);
const REDIS_URL = process.env['REDIS_URL'];
const SCRIPTS_PATH = process.env['SCRIPTS_PATH'];

let config = {};

class QueueRunner {
  constructor(queue, worker_count) {
    this.queue = queue;
    this.workers = {};
    for (let worker = 0; worker < worker_count; worker++) {
      this.workers[worker] = new Worker(this.queue, worker);
      this.workers[worker].start();
    }
    this.quit = this.quit.bind(this);
  }

  quit() {
    console.log(`Shutting down workers for ${this.queue} queue`);
    Object.keys(this.workers).forEach((worker) => {
      this.workers[worker].stop();
    });
  }
}

class Worker {
  constructor(queue, id) {
    this.queue = queue;
    this.id = id;
    this.name = `${os.hostname()}:${process.pid}-${this.id}:${this.queue}`;
    this.interval = parseFloat(config.interval) * 1000;
    this.running = false;
    this.pollTimer = null;

    this.start = this.start.bind(this);
    this.stop = this.stop.bind(this);
    this.waitAndProcess = this.waitAndProcess.bind(this);
    this.register = this.register.bind(this);
    this.unregister = this.unregister.bind(this);
    this.workingOn = this.workingOn.bind(this);
    this.done = this.done.bind(this);
    this.success = this.success.bind(this);
    this.fail = this.fail.bind(this);
  }

  start() {
    this.redis = createRedisClient();
    this.register();
    this.running = true;
    this.waitAndProcess();
  }

  stop() {
    console.log(`Shutting down worker: ${this.name}`);
    clearTimeout(this.pollTimer);
    this.unregister();
    this.running = false;
    this.redis.quit();
  }

  waitAndProcess() {
    this.redis.lpop(`resque:queue:${this.queue}`, (err, res) => {
      const bail = (reason) => {
        console.error(`Worker-Error: ${reason}`);
        this.pollTimer = setTimeout(() => this.waitAndProcess(), this.interval);
      }

      if (err) {
        return bail(err);
      }

      if (!res) {
        this.pollTimer = setTimeout(() => this.waitAndProcess(), this.interval);
        return;
      }

      let job = {}
      try {
        job = JSON.parse(res);
      } catch (e) {
        return bail(`Parse Error: ${e}`);
      }

      this.workingOn(job);

      let response = '';

      const child = fork(path.join(__dirname, './helpers/runHelper'), [config.scriptsPath, res], {silent: true});

      child.stderr.on('data', (errorData) => {
        response += errorData;
      });

      child.on('exit', (code) => {
        if (code === 0) {
          this.success();
        } else {
          this.fail(job, response);
        }
        if (this.running) {
          this.pollTimer = setTimeout(() => this.waitAndProcess(), this.interval);
        }
      });
    });
  }

  register() {
    this.redis.sadd(RK_WORKERS, this.name);
    this.redis.set(`${RK_WORKER}:${this.name}:started`, new Date().toISOString());
  }

  unregister() {
    this.redis.srem(RK_WORKERS, this.name);
    this.redis.del(`${RK_WORKER}:${this.name}`);
    this.redis.del(`${RK_WORKER}:${this.name}:started`);
  }

  workingOn(data) {
    const work = JSON.stringify({
      queue: this.queue,
      run_at: new Date().toISOString(),
      payload: data,
    });
    this.redis.set(`${RK_WORKER}:${this.name}`, work);
  }

  done(successful) {
    const key = successful ? 'processed' : 'failed';
    this.redis.incr(`${RK_STAT}:${key}`);
    this.redis.del(`${RK_WORKER}:${this.name}`);
  }

  success() {
    this.done(true);
  }

  fail(job, response) {
    this.done(false);
    const error = {
      failed_at: new Date().toISOString(),
      payload: job,
      worker: this.name,
      queue: this.queue,
      backtrace: null,
    };
    let info = {};
    try {
      const data = JSON.parse(response);
      JOB_ERROR_TYPES.forEach(errorKey => {
        info[errorKey] = data[errorKey];
      });
    } catch (e) {
      info = {
        exception: 'UnclassifiedRunnerError',
        error: response,
      };
    }
    this.redis.lpush(RK_FAILED, JSON.stringify(Object.assign({}, error, info)));
  }

}

const queues = {};

const createRedisClient = () => {
  const client = redis.createClient(config.redis);
  client.on('error', (err) => {
    console.error(`Error: ${err}`);
  });
  return client;
}

const cleanExit = () => {
  console.log('Unregistering workers');
  Object.keys(queues).forEach(queueName => {
    queues[queueName].quit();
  });
  process.exit(0);
}

const handleException = (error) => {
  console.error(error);
  handleExit({from: 'exception'});
}

const handleExit = ({from}) => {
  console.log(`Exiting from: ${from}`);
  cleanExit();
}

const constructConfigFromEnv = () => {
  const config = {};
  if (INTERVAL) {
    config.interval = INTERVAL;
  }
  if (REDIS_URL) {
    config.redis = REDIS_URL;
  }
  if (QUEUE) {
    const queues = QUEUE.split(',');
    config.queue = queues;
  }
  if (COUNT) {
    config.count = COUNT;
  }
  if (SCRIPTS_PATH) {
    config.scriptsPath = SCRIPTS_PATH;
  }
  return config;
}

const parseScriptsPath = () => {
  if (!path.isAbsolute(config.scriptsPath)) {
    config.scriptsPath = path.join(process.cwd(), config.scriptsPath);
  }
}

const defaultConfig = {
  interval: 5.0,
  redis: 'redis://localhost:6379/0',
  queue: ['default'],
  count: 1,
  scriptsPath: './scripts',
};

const run = (options = {}) => {
  const constructedConfig = constructConfigFromEnv();
  config = Object.assign({}, defaultConfig, constructedConfig, options);
  parseScriptsPath();
  console.log(config);
  config.queue.forEach(queue => {
    queues[queue] = new QueueRunner(queue, config.count);
  });
  process.on('uncaughtException', handleException);
  // process.on('exit', () => handleExit({from: 'exit'}));
  process.on('SIGINT', () => handleExit({from: 'SIGINT'}));
  process.on('SIGHUP', () => handleExit({from: 'SIGHUP'}));
  process.on('SIGTERM', () => handleExit({from: 'SIGTERM'}));
}

exports.run = run;
