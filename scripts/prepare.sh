export VSPACE_HOME=.
export PYARROW_VERSION="0.14.1"
export PYSPARK_VERSION="2.4.3"

mkdir -p $VSPACE_HOME/vspace_alternative
cd $VSPACE_HOME
wget https://repo.continuum.io/miniconda/Miniconda3-4.7.12.1-Linux-x86_64.sh

bash Miniconda3-4.7.12.1-Linux-x86_64.sh -b -p $VSPACE_HOME/.anaconda-root
source $VSPACE_HOME/.anaconda-root/bin/activate
conda config --add channels conda-forge
conda config --set channel_priority strict
conda install -y ipython conda-pack dawg nltk toolz pyarrow=$PYARROW_VERSION pyspark=$PYSPARK_VERSION
conda create -y --prefix $VSPACE_HOME/anaconda dawg nltk toolz pyarrow=$PYARROW_VERSION pyspark=$PYSPARK_VERSION
conda pack -p $VSPACE_HOME/anaconda -o vspace_alternative/anaconda.tgz
