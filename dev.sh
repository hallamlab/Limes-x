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
        cd $HERE/src
        # python -m limes_x $@

        # # setup
        # python -m limes_x setup \
        #     -o ./test/cache/lx_setup \
        #     -m /home/tony/workspace/python/Limes-all/Limes-compute-modules/logistics \
        #     -m /home/tony/workspace/python/Limes-all/Limes-compute-modules/metagenomics /home/tony/workspace/python/Limes-all/Limes-compute-modules/high_throughput_screening \
        #     --blacklist annotation_dram

        # slurm
        python -m limes_x slurm \
            -o /home/tony/workspace/python/Limes-all/Limes-x/test/cache \
            -m /home/tony/workspace/python/Limes-all/Limes-compute-modules/logistics \
            -m /home/tony/workspace/python/Limes-all/Limes-compute-modules/metagenomics \
            -r /home/tony/workspace/python/Limes-all/lx_ref \
            -a alloc \
            -i a b c \
            -t x y z 
    ;;
    *)
        echo "bad option"
        echo $1
    ;;
esac
