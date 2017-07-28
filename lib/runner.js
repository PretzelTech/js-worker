#!/usr/bin/env node

const fs = require('fs');
const path = require('path');
// const spawn = require('child_process').spawn;
const fork = require('child_process').fork;
const os = require('os');
const redis = require('redis');

const queues = {};

const cfgfile = process.env['CONFIG_FILE'] || process.argv[2];
let scriptsPath = process.env['SCRIPT_PATH'] || './scripts';

if (!path.isAbsolute(scriptsPath)) {
  scriptsPath = path.join(process.cwd(), scriptsPath);
}

const defaultConfg = {
  redis: {
    server: "localhost",
    port: 6379,
    db: 2,
  },
  runners: {
    default: 5,
  },
}

let config = {};

if (cfgfile && fs.existsSync(cfgfile)) {
  config = Object.assign({}, defaultConfg, JSON.parse(fs.readFileSync(cfgfile)));
}

class QueueRunner {
  constructor(queue, worker_count) {
    this.queue = queue;
    this.clients = {};
    for (let i = 0; i < worker_count; i++) {
      this.clients[i] = new Worker(this.queue, i);
      this.clients[i].start();
    }
    this.quit = this.quit.bind(this);
  }

  quit() {
    Object.keys(this.clients).forEach((idx) => {
      this.clients[idx].stop();
    });
  }
}

class Worker {
  constructor(queue, id) {
    this.queue = queue;
    this.id = id;
    this.running = false;
    this.start = this.start.bind(this);
    this.stop = this.stop.bind(this);
    this.wait_and_process = this.wait_and_process.bind(this);
    this.worker_name = this.worker_name.bind(this);
    this.register = this.register.bind(this);
    this.unregister = this.unregister.bind(this);
    this.working_on = this.working_on.bind(this);
    this.done = this.done.bind(this);
    this.success = this.success.bind(this);
    this.fail = this.fail.bind(this);
    this.jobErrorTypes = ['exception', 'error', 'backtrace'];
    this.waitTimer = null;
  }

  start() {
    this.redis = createRedisClient();
    this.register();
    this.running = true;
    this.wait_and_process();
  }

  stop() {
    clearTimeout(this.waitTimer);
    this.unregister();
    this.running = false;
    this.redis.quit();
  }

  wait_and_process() {
    // this.redis.blpop(`resque:queue:${this.queue}`, 0, (err, res) => {
    this.redis.lpop(`resque:queue:${this.queue}`, (err, res) => {
      const bail = (reason) => {
        console.error(`Worker-Error: ${reason}`);
        // process.nextTick(this.wait_and_process);
        this.waitTimer = setTimeout(() => this.wait_and_process(), 5000);
      }

      if (err) {
        return bail(err);
      }

      if (!res) {
        this.waitTimer = setTimeout(() => this.wait_and_process(), 5000);
        return;
      }

      let job = {}
      try {
        job = JSON.parse(res);
      } catch (e) {
        return bail(`Parse Error: ${e}`);
      }

      this.working_on(job);

      let response = '';

      //Use a fork of a node proces
      const child = fork(path.join(__dirname, './helpers/runHelper'), [scriptsPath, res]);

      //TODO: Handle child error output in order to properly log it in resque

      // cat.stderr.on('data', (d) => {
      // this doesn't work since a forked child doesn't seem to have stderr prop
      //   response += d;
      // });

      child.on('exit', (code) => {
        if (code === 0) {
          this.success();
        } else {
          this.fail(job, response);
        }
        if (this.running) {
          // process.nextTick(this.wait_and_process);
          this.waitTimer = setTimeout(() => this.wait_and_process(), 5000);
        }
      });
    });
  }

  worker_name() {
    return `${os.hostname()}:${process.pid}-${this.id}:${this.queue}`;
  }

  register() {
    const name = this.worker_name();
    this.redis.sadd('resque:workers', name);
    this.redis.set(`resque:worker:${name}:started`, new Date().toISOString());
  }

  unregister() {
    const name = this.worker_name();
    this.redis.srem('resque:workers', name);
    this.redis.del(`resque:worker:${name}`);
    this.redis.del(`resque:worker:${name}:started`);
  }

  working_on(data) {
    const dataStr = JSON.stringify({
      queue: this.queue,
      run_at: new Date().toISOString(),
      payload: data,
    });
    const name = this.worker_name();
    this.redis.set(`resque:worker:${name}`, dataStr);
  }

  done(successful) {
    const name = this.worker_name();
    const key = successful ? 'processed' : 'failed';
    this.redis.incr("resque:stat:#{key}");
    this.redis.del("resque:worker:#{name}");
  }

  success() {
    this.done(true);
  }

  fail(job, response) {
    this.done(false);
    const name = this.worker_name();
    const error = {
      failed_at: new Date().toISOString(),
      payload: job,
      worker: name,
      queue: this.queue,
      backtrace: null,
    };
    const einfo = {};
    try {
      const data = JSON.parse(response);
      for (key in this.jobErrorTypes) {
        einfo[key] = data[key];
      }
    } catch (e) {
      einfo = {
        exception: 'UnclassifiedRunnerError',
        error: response,
      };
    }
    this.redis.lpush('resque:failed', JSON.stringify(Object.assign({}, error, einfo)));
  }
}

const createRedisClient = () => {
  const options = {};
  if (config && config.redis) {
    options.db = config.redis.db;
  }
  const client = redis.createClient(config.redis.port, config.redis.server, options);
  client.on('error', (err) => {
    console.error(`Error: ${err}`);
  });
  return client;
}

const clean_exit = () => {
  console.log('Unregistering workers');
  Object.keys(queues).forEach((queue) => {
    queues[queue].quit();
  });
  process.exit(0);
}

const run = () => {
  Object.keys(config.runners).forEach((queue) => {
    queues[queue] = new QueueRunner(queue, config.runners[queue]);
  });

  const sigArray = ['INT', 'HUP', 'TERM'];

  for (let sig in sigArray) {
    process.on(`SIG${sigArray[sig]}`, clean_exit);
  }

  process.on('uncaughtException', function (err) {
    console.error(err);
    clean_exit();
  });
}

exports.run = run;
