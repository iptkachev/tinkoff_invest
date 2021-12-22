Heroku python tutorial https://devcenter.heroku.com/articles/getting-started-with-python

Heroku env tutorial https://devcenter.heroku.com/articles/config-vars

- Publish new version of master `git push heroku master`
- Start scheduler `heroku ps:scale worker=1`
- Stop scheduler `heroku ps:scale worker=0`