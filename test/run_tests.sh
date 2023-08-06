
HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd $HERE
PYTHONPATH=$HERE/../src/:$PYTHONPATH

export SKIP_SOLVER=1
pytest
