# NAME=limes_x
# DOCKER_IMAGE=quay.io/txyliu/$NAME
# echo image: $DOCKER_IMAGE
# echo ""

HERE=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

case $1 in
    --pip-setup)
        # make an environment before hand
        # in that env, install these build tools
        pip install twine build
    ;;
    --pip-install|-i)
        # install the package locally
        python setup.py install
    ;;
    --pip-build|-b)
        # build the packge for upload to pypi
        rm -r build && rm -r dist
        python -m build --wheel
    ;;
    --pip-upload|-u)
        # upload to pypi
        # use testpypi for dev
        # PYPI=testpypi
        PYPI=pypi
        TOKEN=`cat secrets/${PYPI}`
        python -m twine upload --repository $PYPI dist/* -u __token__ -p $TOKEN
    ;;
    --pip-remove|-x)
        pip uninstall -y limes_x
    ;;
    --test|-t)
        shift
        cd $HERE/test
        export SLURM_TMPDIR=/home/tony/workspace/python/Limes-all/Limes-x/test/cache/temp
        PATH=$HERE/test/mock:$PATH
        PYTHONPATH=$HERE/src:$PYTHONPATH
        python preset_slurm.py
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
