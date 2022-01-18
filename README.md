# MBDProject10

## Initializing Git repository
* SSH into the ewi.utwente.nl cluster
* Run `ssh-keygen -t ed25519 -C "github"`
* Go to the ssh directory where the public key is saved
* Copy the contents of the .pub file
* Go to your GitHub settings (of your profile, not this project)
* Go to SSH and GPG keys
* Add your copied key
* On the cluster, run `git clone git@github.com:EricvanSchaik/MBDProject10.git`
* To make sure that git doesn't push all the output of the notebooks (this can become very large), run
```console
git config filter.strip-notebook-output.clean 'jupyter nbconvert --ClearOutputPreprocessor.enabled=True --to=notebook --stdin --stdout --log-level=ERROR'
```


## Install Jupyter on the MBD server

### Run these commands on the server over ssh
```console
python3 -m pip install jupyter
touch ~/.bashrc
echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> ~/.bashrc
echo 'export PATH="~/.local/bin:$PATH"' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON="/home/[your student number]/.local/jupyter"' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8080"' >> ~/.bashrc
source ~/.bashrc
```

And then to start the notebook just run

```console
pyspark
```

### Run these commands on your local computer
This is to forward the remote localhost to your own
```console
ssh -N -L 8080:localhost:8080 [yourstudentnumber]@ctit[yourservernumber].ewi.utwente.nl
```

Then open the link outputted by the `pyspark` command in your browser and it should work

## Install required packages
```console
python3 -m pip install numpy
python3 -m pip install pandas
python3 -m pip install matplotlib
```
