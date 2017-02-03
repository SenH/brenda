from setuptools import setup, Extension, Feature

# original version
VERSION="0.5"

# get current version
with open('brenda/version.py') as f:
    exec(f.read())

paracurl = Feature(
    'Paracurl',
    ext_modules = [Extension("paracurl", ["paracurl/paracurl.c"], libraries=['curl'])]
)

setup(name = "Brenda",
      version = VERSION,
      packages = ['brenda'],
      scripts = ['brenda-work', 'brenda-tool', 'brenda-run', 'brenda-node', 'brenda-ebs'],
      features = {'paracurl': paracurl},
      author = "James Yonan",
      author_email = "james@openvpn.net",
      description = "Render farm tool for Amazon Web Services",
)
