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
        # build and install the package locally
        python setup.py build \
        && python setup.py install
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
    -t)
        echo "hi"
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
