
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $HERE
PYTHONPATH=$HERE/../src/:$PYTHONPATH
pytest
