import pytest

import os

from waterbutler.providers.googledriveinstitutions.provider import GoogleDriveInstitutionsPath
from waterbutler.providers.googledriveinstitutions.provider import GoogleDriveInstitutionsPathPart
from waterbutler.providers.googledriveinstitutions.metadata import GoogleDriveInstitutionsRevision
from waterbutler.providers.googledriveinstitutions.metadata import GoogleDriveInstitutionsFileMetadata
from waterbutler.providers.googledriveinstitutions.metadata import GoogleDriveInstitutionsFolderMetadata

from tests.providers.googledriveinstitutions.fixtures import(
    error_fixtures,
    root_provider_fixtures,
    revision_fixtures,
    sharing_fixtures,
)


@pytest.fixture
def basepath():
    return GoogleDriveInstitutionsPath('/conrad')


class TestMetadata:

    def test_file_metadata_drive(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['list_file']['files'][0]
        path = basepath.child(item['name'])
        parsed = GoogleDriveInstitutionsFileMetadata(item, path)

        assert parsed.provider == 'googledriveinstitutions'
        assert parsed.id == item['id']
        assert path.name == item['name']
        assert parsed.name == item['name']
        assert parsed.size_as_int == 918668
        assert type(parsed.size_as_int) == int
        assert parsed.size == item['size']
        assert parsed.modified == item['modifiedTime']
        assert parsed.content_type == item['mimeType']
        assert parsed.extra == {
            'revisionId': item['version'],
            'webView': item['webViewLink'],
            'hashes': {'md5': item['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_google_doc is False
        assert parsed.export_name == item['name']

    def test_file_metadata_drive_slashes(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['file_forward_slash']
        path = basepath.child(item['name'])
        parsed = GoogleDriveInstitutionsFileMetadata(item, path)

        assert parsed.provider == 'googledriveinstitutions'
        assert parsed.id == item['id']
        assert parsed.name == item['name']
        assert parsed.name == path.name
        assert parsed.size == item['size']
        assert parsed.size_as_int == 918668
        assert type(parsed.size_as_int) == int
        assert parsed.modified == item['modifiedTime']
        assert parsed.content_type == item['mimeType']
        assert parsed.extra == {
            'revisionId': item['version'],
            'webView': item['webViewLink'],
            'hashes': {'md5': item['md5Checksum']},
        }
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts])
        assert parsed.materialized_path == str(path)
        assert parsed.is_google_doc is False
        assert parsed.export_name == item['name']

    def test_file_metadata_docs(self, basepath, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']
        path = basepath.child(item['name'])
        parsed = GoogleDriveInstitutionsFileMetadata(item, path)

        assert parsed.name == item['name'] + '.gdoc'
        assert parsed.extra == {
            'revisionId': item['version'],
            'downloadExt': '.docx',
            'webView': item['webViewLink'],
        }
        assert parsed.is_google_doc is True
        assert parsed.export_name == item['name'] + '.docx'

    def test_folder_metadata(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        path = GoogleDriveInstitutionsPath('/we/love/you/conrad').child(item['name'], folder=True)
        parsed = GoogleDriveInstitutionsFolderMetadata(item, path)

        assert parsed.provider == 'googledriveinstitutions'
        assert parsed.id == item['id']
        assert parsed.name == item['name']
        assert parsed.extra == {'revisionId': item['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == item['name']

    def test_folder_metadata_slash(self, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata_forward_slash']
        path = GoogleDriveInstitutionsPath('/we/love/you/conrad').child(item['name'], folder=True)
        parsed = GoogleDriveInstitutionsFolderMetadata(item, path)

        assert parsed.provider == 'googledriveinstitutions'
        assert parsed.id == item['id']
        assert parsed.name == item['name']
        assert parsed.extra == {'revisionId': item['version']}
        assert parsed.path == '/' + os.path.join(*[x.raw for x in path.parts]) + '/'
        assert parsed.materialized_path == str(path)
        assert parsed.export_name == item['name']

    def test_revision_metadata(self, revision_fixtures):
        item = revision_fixtures['revision_metadata']
        parsed = GoogleDriveInstitutionsRevision(item)
        assert parsed.version_identifier == 'revision'
        assert parsed.version == item['id']
        assert parsed.modified == item['modifiedTime']
