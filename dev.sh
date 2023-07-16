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
    --env-dev)
        conda env remove -n lx_dev
        mamba env create --no-default-packages -f $HERE/env_dev.yml
    ;;
    --env)
        conda env remove -n lx
        mamba env create --no-default-packages -f $HERE/env_dev.yml
    ;;
    --run|-r)
        cd $HERE/src
        shift
        python -m limes_x $@
        cd $HERE
        # PYTHONPATH=$HERE/src:$PYTHONPATH
        # python -c "from limes_x.cli import main; main()"
    ;;
    --test|-t)
        cd $HERE/src
        shift
        python -m limes_x outpost -c $HERE/test/outposts/local_outpost/config.yml
        cd $HERE
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
