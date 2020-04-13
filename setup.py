import setuptools
from os_task_scheduler import version

package_name = "os_task_scheduler"
setuptools.setup(
    name=package_name,
    url="https://github.com/final-israel/{}/".format(package_name),
    version=version.version,
    description="Library that spinup and manage Openstack stacks and schedule tasks to be performed on them",
    author_email="zoharkol@gmail.com",
    author="zohar",
    long_description=open("README").read(),
    install_requires=["pika>=0.13.1", "asyncio"],
    python_requires=">=3.5",
    packages=[package_name],
)