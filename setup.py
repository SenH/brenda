from setuptools import setup

# original version
VERSION="0.5"

# get current version
with open('brenda/version.py') as f:
    exec(f.read())

setup(name = "Brenda",
      version = VERSION,
      packages = ['brenda'],
      scripts = ['brenda-work', 'brenda-tool', 'brenda-run', 'brenda-node'],
      author = "Sen Haerens",
      author_email = "sen@senhaerens.be",
      description = "Render farm tool for cloud computing services",
)
