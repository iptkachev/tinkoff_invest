## Heroku

Heroku python tutorial https://devcenter.heroku.com/articles/getting-started-with-python

Heroku env tutorial https://devcenter.heroku.com/articles/config-vars

- Publish new version of master `git push heroku master`
- Start scheduler `heroku ps:scale worker=1`
- Stop scheduler `heroku ps:scale worker=0`

## Supervisor

- `sudo vi /etc/supervisor/conf.d/process.conf` - add process
- `sudo supervisorctl reload` - reload supervisor after each editing
- `sudo supervisorctl status` - check status of processes
- `sudo supervisorctl restart process_00` - restart process by name __process_00__
- `sudo supervisorctl stop process_00` - stop process by name __process_00__
- `sudo supervisorctl start process_00` - start process by name __process_00__
- `sudo supervisorctl fg process_00` - open fg process and view stdout|stderr by name __process_00__
