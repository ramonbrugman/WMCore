from sets import Set
from WMCore.DataStructs.WMObject import WMObject
class JobFactory(WMObject):
    """
    A JobFactory is created with a subscription (which has a fileset). It is a
    base class of an object instance of an object representing some job splitting 
    algorithm. It is called with a job type (at least) to return a Set of Job 
    objects. The JobFactory should be subclassed by real splitting algorithm
    implementations.
    """
    def __init__(self, package='WMCore.DataStructs', subscription=None):
        self.package = package
        self.subscription = subscription
        
    def __call__(self, jobtype='Job', *args, **kwargs):
        """
        The default behaviour of JobFactory.__call__ is to return a single
        Job associated with all the files in the subscription's fileset
        """
        module = "%s.%s" % (self.package, jobtype)
        module = __import__(module, globals(), locals(), [jobtype])#, -1)
        job_instance = getattr(module, jobtype.split('.')[-1])
        return self.algorithm(job_instance=job_instance, *args, **kwargs)
    
    def algorithm(self, job_instance=None, *args, **kwargs):
        return Set([job_instance(self.subscription, self.subscription.availableFiles())])