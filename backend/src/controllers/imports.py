from ..services import imports
class ImportsController:
    create=staticmethod(imports.create_import)
    list=staticmethod(imports.list_imports)
    status=staticmethod(imports.status)
    chunk=staticmethod(imports.upload_chunk)
    complete=staticmethod(imports.complete_import)
    retry=staticmethod(imports.retry_import)
    cancel=staticmethod(imports.cancel_import)
