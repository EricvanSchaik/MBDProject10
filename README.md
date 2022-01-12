# MBDProject10

## Initialzing Git repository
* SSH into the ewi.utwente.nl cluster
* Create a new directory in your own home directory
* Run `git init`
* Run `ssh-keygen -t ed25519 -C "github"`
* Go to the ssh directory where the public key is saved
* Copy the contents of the .pub file
* Go to your GitHub settings (of your profile, not this project)
* Go to SSH and GPG keys
* Add your copied key
* On the cluster, in the git folder, run `git remote add origin git@github.com:EricvanSchaik/MBDProject10.git`
