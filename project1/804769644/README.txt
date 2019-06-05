There were no issues. Sometimes when received a 502 error, I performed the commands: 

touch /tmp/searchengine.sock
sudo reboot

in the root level directory and that fixed the issue. 

If that did not fix the issue, run :

sudo systemctl restart searchengine

and that will solve it.