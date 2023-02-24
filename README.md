# bork (experimental)

⚠⚠⚠️ DO NOT RUN THIS ON YOUR ONLY BACKUP REPOSITORY, DO NOT USE THIS TO PERFORM BACKUPS OF DATA YOU CARE ABOUT ️⚠⚠⚠️️️️️️️️

bork is an exploration of implementing parts of [BorgBackup](https://www.borgbackup.org) in Rust for the purpose of improved performance in terms of runtime throughput and memory usage.

## goals

- learn about how Borg structures its repositories
- make certain parts of Borg workflows faster/more memory efficient (eventually)
- being "good" as defined by me

## non-goals

- supporting all of Borg's encryption/compression options
- implementing the entirety of the Borg CLI
- being a reliable tool for backups
- replacing borg
- preventing the deletion/irreversible corruption of your data (see above warning)
- being "good" as defined by you
