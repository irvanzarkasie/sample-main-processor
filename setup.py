import setuptools

try:
    # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:
    # for pip <= 9.0.3
    from pip.req import parse_requirements

def load_requirements(fname):
    reqs = parse_requirements(fname, session="test")
    return [str(ir.req) for ir in reqs]

setuptools.setup(
    name="sample-processor",
    version="0.0.1",
    author="expecc",
    author_email="expecc@expecc.com",
    description="Sample processor",
    packages=setuptools.find_packages(),
    include_package_data=True
)
