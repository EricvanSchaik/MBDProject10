# MBDProject10

## Install Jupyter on the MBD server

### Run these commands on the server over ssh
```console
python3 -m pip install jupyter
touch ~/.bashrc
echo 'export PYSPARK_PYTHON=/usr/bin/python3' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON=python3' >> ~/.bashrc
echo 'export PATH="~/.local/bin:$PATH"' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON="jupyter"' >> ~/.bashrc
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
