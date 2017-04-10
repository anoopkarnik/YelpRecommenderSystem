#!/bin/sh
git add .
git status 
git commit -m "Fourth Commit"
expect "Username for 'https://github.com' :" { send "anoopkarnik\r" }
git push --all