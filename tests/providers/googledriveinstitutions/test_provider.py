import io
import os
import copy
import json
from http import client
from urllib import parse
import hashlib

import pytest
import aiohttpretty

from waterbutler.core import streams
from waterbutler.core import exceptions
from waterbutler.core.path import WaterButlerPath

from waterbutler.providers.googledriveinstitutions import settings as ds
from waterbutler.providers.googledriveinstitutions import GoogleDriveInstitutionsProvider
from waterbutler.providers.googledriveinstitutions import utils as drive_utils
from waterbutler.providers.googledriveinstitutions.provider import GoogleDriveInstitutionsPath
from waterbutler.providers.googledriveinstitutions.metadata import (GoogleDriveInstitutionsRevision,
                                                        GoogleDriveInstitutionsFileMetadata,
                                                        GoogleDriveInstitutionsFolderMetadata,
                                                        GoogleDriveInstitutionsFileRevisionMetadata)

from tests.providers.googledriveinstitutions.fixtures import(error_fixtures,
                                                 sharing_fixtures,
                                                 revision_fixtures,
                                                 root_provider_fixtures)


@pytest.fixture
def file_content():
    return b'SLEEP IS FOR THE WEAK GO SERVE STREAMS'


@pytest.fixture
def file_like(file_content):
    return io.BytesIO(file_content)


@pytest.fixture
def file_stream(file_like):
    return streams.FileStreamReader(file_like)


@pytest.fixture
def auth():
    return {
        'name': 'cat',
        'email': 'cat@cat.com',
    }


@pytest.fixture
def credentials():
    return {'token': 'hugoandkim'}


@pytest.fixture
def other_credentials():
    return {'token': 'hugoandprobablynotkim'}


@pytest.fixture
def settings():
    return {
        'folder': {
            'id': '19003e',
            'name': '/conrad/birdie',
        },
    }


@pytest.fixture
def provider(auth, credentials, settings):
    return GoogleDriveInstitutionsProvider(auth, credentials, settings)


@pytest.fixture
def other_provider(auth, other_credentials, settings):
    return GoogleDriveInstitutionsProvider(auth, other_credentials, settings)


@pytest.fixture
def search_for_file_response():
    return {
        'files': [
            {'id': '1234ideclarethumbwar'}
        ]
    }


@pytest.fixture
def no_file_response():
    return {
        'files': []
    }


@pytest.fixture
def actual_file_response():
    return {
        'id': '1234ideclarethumbwar',
        'mimeType': 'text/plain',
        'name': 'B.txt',
    }


@pytest.fixture
def search_for_folder_response():
    return {
        'files': [
            {'id': 'whyis6afraidof7'}
        ]
    }


@pytest.fixture
def no_folder_response():
    return {
        'files': []
    }


@pytest.fixture
def actual_folder_response():
    return {
        'id': 'whyis6afraidof7',
        'mimeType': 'application/vnd.google-apps.folder',
        'name': 'A',
    }


def make_unauthorized_file_access_error(file_id):
    message = ('The authenticated user does not have the required access '
               'to the file {}'.format(file_id))
    return json.dumps({
        "error": {
            "errors": [
                {
                    "reason": "userAccess",
                    "locationType": "header",
                    "message": message,
                    "location": "Authorization",
                    "domain": "global"
                }
            ],
            "message": message,
            "code": 403
        }
    })


def make_no_such_revision_error(revision_id):
    message = 'Revision not found: {}'.format(revision_id)
    return json.dumps({
        "error": {
            "errors": [
                {
                    "reason": "notFound",
                    "locationType": "other",
                    "message": message,
                    "location": "revision",
                    "domain": "global"
                }
            ],
            "message": message,
            "code": 404
        }
    })


def clean_query(query: str):
    # Replace \ with \\ and ' with \'
    # Note only single quotes need to be escaped
    return query.replace('\\', r'\\').replace("'", r"\'")


def _build_name_search_query(provider, entity_name, file_id, is_folder=True):
    return "name = '{}' " \
        "and trashed = false " \
        "and mimeType != 'application/vnd.google-apps.form' " \
        "and mimeType != 'application/vnd.google-apps.map' " \
        "and mimeType != 'application/vnd.google-apps.document' " \
        "and mimeType != 'application/vnd.google-apps.drawing' " \
        "and mimeType != 'application/vnd.google-apps.presentation' " \
        "and mimeType != 'application/vnd.google-apps.spreadsheet' " \
        "and mimeType {} '{}' " \
        "and '{}' in parents".format(
            entity_name,
            '=' if is_folder else '!=',
            provider.FOLDER_MIME_TYPE,
            file_id
        )


def generate_list(child_id, root_provider_fixtures, **kwargs):
    item = {}
    item.update(root_provider_fixtures['list_file']['files'][0])
    item.update(kwargs)
    item['id'] = str(child_id)
    return {'files': [item]}


class TestValidatePath:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_file(self, provider, search_for_file_response,
                                         actual_file_response, no_folder_response):
        file_name = 'file.txt'
        file_id = '1234ideclarethumbwar'

        url = provider.build_url('files')
        query_params = {'q': _build_name_search_query(provider, file_name, provider.folder['id'], False),
                        'fields': 'files(id)'}
        wrong_query_params = {'q': _build_name_search_query(provider, file_name, provider.folder['id'], True),
                              'fields': 'files(id)'}
        specific_url = provider.build_url('files', file_id)
        specific_params = {'fields': 'id,name,mimeType'}

        aiohttpretty.register_json_uri('GET', url, params=query_params, body=search_for_file_response)
        aiohttpretty.register_json_uri('GET', url, params=wrong_query_params, body=no_folder_response)
        aiohttpretty.register_json_uri('GET', specific_url, params=specific_params, body=actual_file_response)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + file_name)
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + file_name + '/')

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + file_name)

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_folder(self, provider, search_for_folder_response,
                                           actual_folder_response, no_file_response):
        folder_name = 'foofolder'
        folder_id = 'whyis6afraidof7'

        url = provider.build_url('files')
        query_params = {'q': _build_name_search_query(provider, folder_name, provider.folder['id'], True),
                        'fields': 'files(id)'}
        wrong_query_params = {'q': _build_name_search_query(provider, folder_name, provider.folder['id'], False),
                              'fields': 'files(id)'}
        specific_url = provider.build_url('files', folder_id)
        specific_params = {'fields': 'id,name,mimeType'}

        aiohttpretty.register_json_uri('GET', url, params=query_params, body=search_for_folder_response)
        aiohttpretty.register_json_uri('GET', url, params=wrong_query_params, body=no_file_response)
        aiohttpretty.register_json_uri('GET', specific_url, params=specific_params, body=actual_folder_response)

        try:
            wb_path_v1 = await provider.validate_v1_path('/' + folder_name + '/')
        except Exception as exc:
            pytest.fail(str(exc))

        with pytest.raises(exceptions.NotFoundError) as exc:
            await provider.validate_v1_path('/' + folder_name)

        assert exc.value.code == client.NOT_FOUND

        wb_path_v0 = await provider.validate_path('/' + folder_name + '/')

        assert wb_path_v1 == wb_path_v0

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_validate_v1_path_root(self, provider):
        path = '/'

        result = await provider.validate_v1_path(path)
        expected = GoogleDriveInstitutionsPath('/', _ids=[provider.folder['id']], folder=True)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file(self, provider, root_provider_fixtures):
        file_name = '/Gear1.stl'
        revalidate_path_metadata = root_provider_fixtures['revalidate_path_file_metadata_1']
        file_id = revalidate_path_metadata['files'][0]['id']
        path = GoogleDriveInstitutionsPath(file_name, _ids=['0', file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False

        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        name, ext = os.path.splitext(part_name)
        query = _build_name_search_query(provider, file_name.strip('/'), file_id, False)

        url = provider.build_url('files')
        params = {'q': query, 'fields': 'files(id)'}
        aiohttpretty.register_json_uri('GET', url, params=params, body=revalidate_path_metadata)

        url = provider.build_url('files', file_id)
        params = {'fields': 'id,name,mimeType'}
        aiohttpretty.register_json_uri('GET', url, params=params,
                                       body=root_provider_fixtures['revalidate_path_file_metadata_2'])

        result = await provider.revalidate_path(path, file_name)

        assert result.name in path.name

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file_gdoc(self, provider, root_provider_fixtures):
        file_name = '/Gear1.gdoc'
        file_id = root_provider_fixtures['revalidate_path_file_metadata_1']['files'][0]['id']
        path = GoogleDriveInstitutionsPath(file_name, _ids=['0', file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False

        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        name, ext = os.path.splitext(part_name)
        gd_ext = drive_utils.get_mimetype_from_ext(ext)
        query = "name = '{}' " \
                "and trashed = false " \
                "and mimeType = '{}' " \
                "and '{}' in parents".format(clean_query(name), gd_ext, file_id)

        url = provider.build_url('files')
        params = {'q': query, 'fields': 'files(id)'}
        aiohttpretty.register_json_uri('GET', url, params=params,
                                       body=root_provider_fixtures['revalidate_path_file_metadata_1'])

        url = provider.build_url('files', file_id)
        params = {'fields': 'id,name,mimeType'}
        aiohttpretty.register_json_uri('GET', url, params=params,
                                       body=root_provider_fixtures['revalidate_path_gdoc_file_metadata'])

        result = await provider.revalidate_path(path, file_name)

        assert result.name in path.name

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_folder(self, provider, root_provider_fixtures):
        file_name = "/inception folder yo/"
        file_id = root_provider_fixtures['revalidate_path_folder_metadata_1']['files'][0]['id']
        path = GoogleDriveInstitutionsPath(file_name, _ids=['0', file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False

        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        name, ext = os.path.splitext(part_name)
        query = _build_name_search_query(provider, file_name.strip('/') + '/', file_id, True)

        folder_one_url = provider.build_url('files')
        folder_one_params = {'q': query, 'fields': 'files(id)'}
        aiohttpretty.register_json_uri('GET', folder_one_url, params=folder_one_params,
                                       body=root_provider_fixtures['revalidate_path_folder_metadata_1'])

        folder_two_url = provider.build_url('files', file_id)
        folder_two_params = {'fields': 'id,name,mimeType'}
        aiohttpretty.register_json_uri('GET', folder_two_url, params=folder_two_params,
                                       body=root_provider_fixtures['revalidate_path_folder_metadata_2'])

        result = await provider.revalidate_path(path, file_name, True)
        assert result.name in path.name


class TestUpload:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], None))

        url = provider._build_upload_url('files')
        start_upload_params = {'uploadType': 'resumable',
                               'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                         'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        finish_upload_params = {'uploadType': 'resumable', 'upload_id': upload_id,
                                'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                          'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        aiohttpretty.register_json_uri('PUT', url, params=finish_upload_params, body=item)
        aiohttpretty.register_uri('POST', url, params=start_upload_params,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})

        file_stream.add_writer('sha512', streams.HashStreamWriter(hashlib.sha512))
        result, created = await provider.upload(file_stream, path)

        item['sha512'] = file_stream.writers['sha512'].hexdigest
        expected = GoogleDriveInstitutionsFileMetadata(item, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=url, params=start_upload_params)
        assert aiohttpretty.has_call(method='PUT', uri=url, params=finish_upload_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_doesnt_unquote(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        item = root_provider_fixtures['list_file']['files'][0]
        path = GoogleDriveInstitutionsPath('/birdie%2F %20".jpg', _ids=(provider.folder['id'], None))

        url = provider._build_upload_url('files')
        start_upload_params = {'uploadType': 'resumable',
                               'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                         'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        finish_upload_params = {'uploadType': 'resumable', 'upload_id': upload_id,
                                'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                          'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        aiohttpretty.register_json_uri('PUT', url, params=finish_upload_params, body=item)
        aiohttpretty.register_uri('POST', url, params=start_upload_params,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})

        file_stream.add_writer('sha512', streams.HashStreamWriter(hashlib.sha512))
        result, created = await provider.upload(file_stream, path)

        item['sha512'] = file_stream.writers['sha512'].hexdigest
        expected = GoogleDriveInstitutionsFileMetadata(item, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=url, params=start_upload_params)
        assert aiohttpretty.has_call(method='PUT', uri=url, params=finish_upload_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_update(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], item['id']))

        url = provider._build_upload_url('files', path.identifier)
        start_upload_params = {'uploadType': 'resumable',
                               'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                         'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        finish_upload_params = {'uploadType': 'resumable', 'upload_id': upload_id,
                                'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                          'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        aiohttpretty.register_json_uri('PUT', url, params=finish_upload_params, body=item)
        aiohttpretty.register_uri('PATCH', url, params=start_upload_params,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})

        file_stream.add_writer('sha512', streams.HashStreamWriter(hashlib.sha512))
        result, created = await provider.upload(file_stream, path)

        item['sha512'] = file_stream.writers['sha512'].hexdigest
        expected = GoogleDriveInstitutionsFileMetadata(item, path)

        assert created is False
        assert result == expected
        assert aiohttpretty.has_call(method='PATCH', uri=url, params=start_upload_params)
        assert aiohttpretty.has_call(method='PUT', uri=url, params=finish_upload_params)



    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_upload_create_nested(self, provider, file_stream, root_provider_fixtures):
        upload_id = '7'
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath(
            '/ed/sullivan/show.mp3',
            _ids=[str(x) for x in range(3)]
        )

        url = provider._build_upload_url('files')
        start_upload_params = {'uploadType': 'resumable',
                               'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                         'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        finish_upload_params = {'uploadType': 'resumable', 'upload_id': upload_id,
                                'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                          'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        aiohttpretty.register_uri('POST', url, params=start_upload_params,
                                  headers={'LOCATION': 'http://waterbutler.io?upload_id={}'.format(upload_id)})
        aiohttpretty.register_json_uri('PUT', url, params=finish_upload_params, body=item)

        file_stream.add_writer('sha512', streams.HashStreamWriter(hashlib.sha512))
        result, created = await provider.upload(file_stream, path)

        item['sha512'] = file_stream.writers['sha512'].hexdigest
        expected = GoogleDriveInstitutionsFileMetadata(item, path)

        assert created is True
        assert result == expected
        assert aiohttpretty.has_call(method='POST', uri=url, params=start_upload_params)
        assert aiohttpretty.has_call(method='PUT', uri=url, params=finish_upload_params)


class TestDelete:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(None, item['id']))
        delete_url = provider.build_url('files', item['id'])
        aiohttpretty.register_uri('DELETE',
                                  delete_url,
                                  status=204)

        result = await provider.delete(path)

        assert result is None
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_folder(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        del_url = provider.build_url('files', item['id'])

        path = WaterButlerPath('/foobar/', _ids=('doesntmatter', item['id']))

        aiohttpretty.register_uri('DELETE',
                                  del_url,
                                  status=204)

        _ = await provider.delete(path)

        assert aiohttpretty.has_call(method='DELETE', uri=del_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_not_existing(self, provider):
        with pytest.raises(exceptions.NotFoundError):
            await provider.delete(WaterButlerPath('/foobar/'))

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root_no_confirm(self, provider):
        path = WaterButlerPath('/', _ids=('0'))

        with pytest.raises(exceptions.DeleteError) as e:
            await provider.delete(path)

        assert e.value.message == 'confirm_delete=1 is required for deleting root provider folder'
        assert e.value.code == 400

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_delete_root(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['delete_contents_metadata']['files'][0]
        root_path = WaterButlerPath('/', _ids=('0'))

        url = provider.build_url('files')
        params = {'q': "'{}' in parents".format('0'), 'fields': 'files(id)'}
        aiohttpretty.register_json_uri('GET', url, params=params,
                                       body=root_provider_fixtures['delete_contents_metadata'])

        delete_url = provider.build_url('files', item['id'])
        aiohttpretty.register_json_uri('DELETE', delete_url, status=204)

        await provider.delete(root_path, 1)

        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)


class TestDownload:
    """Google Docs (incl. Google Sheets, Google Slides, etc.) require extra API calls and use a
    different branch for downloading/exporting files than non-GDoc files.  For brevity's sake
    our non-gdoc test files are called jpegs, though it could stand for any type of file.

    We want to test all the permutations of:

    * editability: canEdit vs. viewable files
    * file type: Google doc vs. non-Google Doc (e.g. jpeg)
    * revision parameter: non, valid, invalid, and magic

    Non-canEdit (viewable) GDocs do not support revisions, so the good and bad revisions tests
    are the same.  Both should 404.

    The notion of a GDOC_GOOD_REVISION being the same as a JPEG_BAD_REVISION and vice-versa is an
    unnecessary flourish for testing purposes.  I'm only including it to remind developers that
    GDoc revisions look very different from non-GDoc revisions in production.
    """

    GDOC_GOOD_REVISION = '1'
    GDOC_BAD_REVISION = '0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ'
    JPEG_GOOD_REVISION = GDOC_BAD_REVISION
    JPEG_BAD_REVISION = GDOC_GOOD_REVISION
    MAGIC_REVISION = '"LUxk1DXE_0fd4yeJDIgpecr5uPA/MTQ5NTExOTgxMzgzOQ"{}'.format(
        ds.DRIVE_IGNORE_VERSION)

    GDOC_EXPORT_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        revisions_body = sharing_fixtures['canEdit_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)
        assert result.name == 'canEdit_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url, params=revisions_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_gdoc_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = sharing_fixtures['canEdit_gdoc']['revision']

        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_GOOD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = revision_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.GDOC_GOOD_REVISION)
        assert result.name == 'canEdit_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=revision_url, params=revision_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = make_no_such_revision_error(self.GDOC_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        revisions_body = sharing_fixtures['canEdit_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)
        assert result.name == 'canEdit_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url, params=revisions_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path)
        assert result.name == 'viewable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)
        assert result.name == 'viewable_gdoc.docx'

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', path.identifier)
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.download(path)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_jpeg_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = sharing_fixtures['canEdit_jpeg']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_GOOD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'],
                                               'revisions', self.JPEG_GOOD_REVISION)
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.JPEG_GOOD_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=revision_url, params=revision_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = make_no_such_revision_error(self.JPEG_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_canEdit_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', path.identifier)
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', path.identifier)
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.download(path)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.download(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_viewable_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', path.identifier)
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.download(path, revision=self.MAGIC_REVISION)

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_download_range(self, provider, sharing_fixtures):
        """This test is adapted from test_canEdit_jpeg_no_revision"""
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we'
        download_file_url = provider.build_url('files', path.identifier)
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params,
                                  body=file_content, auto_length=True, status=206)

        result = await provider.download(path, range=(0,1))
        assert result.partial

        content = await result.read()
        assert content == file_content
        assert aiohttpretty.has_call(method='GET', uri=download_file_url,
                                     params=download_file_params,
                                     headers={'Range': 'bytes=0-1',
                                              'authorization': 'Bearer hugoandkim'})


class TestMetadata:
    """Google Docs (incl. Google Sheets, Google Slides, etc.) require extra API calls and use a
    different branch for fetching metadata about files than non-GDoc files.  For brevity's sake
    our non-gdoc test files are called jpegs, though it could stand for any type of file.

    We want to test all the permutations of:

    * editability: canEdit vs. viewable files
    * file type: Google doc vs. non-Google Doc (e.g. jpeg)
    * revision parameter: non, valid, invalid, and magic

    Non-canEdit (viewable) GDocs do not support revisions, so the good and bad revisions tests
    are the same.  Both should 404.

    The notion of a GDOC_GOOD_REVISION being the same as a JPEG_BAD_REVISION and vice-versa is an
    unnecessary flourish for testing purposes.  I'm only including it to remind developers that
    GDoc revisions look very different from non-GDoc revisions in production.
    """

    GDOC_GOOD_REVISION = '1'
    GDOC_BAD_REVISION = '0B74RCNS4TbRVTitFais4VzVmQlQ4S0docGlhelk5MXE3OFJnPQ'
    JPEG_GOOD_REVISION = GDOC_BAD_REVISION
    JPEG_BAD_REVISION = GDOC_GOOD_REVISION
    MAGIC_REVISION = '"LUxk1DXE_0fd4yeJDIgpecr5uPA/MTQ5NTExOTgxMzgzOQ"{}'.format(
        ds.DRIVE_IGNORE_VERSION)

    GDOC_EXPORT_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root(self, provider, root_provider_fixtures):
        file_metadata = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], file_metadata['id']))

        list_file_url = provider.build_url('files', path.identifier)
        list_file_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                      'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', list_file_url, params=list_file_params, body=file_metadata)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', file_metadata['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        file_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(file_metadata, path)

        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_string_error_response(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/birdie.jpg',
                               _ids=(provider.folder['id'],
                                     root_provider_fixtures['list_file']['files'][0]['id']))

        list_file_url = provider.build_url('files', path.identifier)
        list_file_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                      'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_uri('GET', list_file_url, params=list_file_params, headers={'Content-Type': 'text/html'},
            body='this is an error message string with a 404... or is it?', status=404)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path)

        assert e.value.code == 404
        assert e.value.message == 'Could not retrieve file or directory {}'.format('/' + path.path)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_root_not_found(self, provider):
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], None))

        with pytest.raises(exceptions.MetadataError) as exc_info:
            await provider.metadata(path)

        assert exc_info.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_file_nested(self, provider, root_provider_fixtures):
        path = GoogleDriveInstitutionsPath(
            '/hugo/kim/pins',
            _ids=[str(x) for x in range(4)]
        )

        item = generate_list(3, root_provider_fixtures)['files'][0]
        url = provider.build_url('files', path.identifier)
        params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                            'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        aiohttpretty.register_json_uri('GET', url, params=params, body=item)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', item['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        item['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(item, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=url, params=params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_root_folder(self, provider, root_provider_fixtures):
        path = await provider.validate_path('/')
        body = root_provider_fixtures['list_file']
        item = body['files'][0]
        query = provider._build_query(provider.folder['id'])
        list_file_url = provider.build_url('files')
        list_file_params = {'q': query, 'alt':'json', 'pageSize': '1000',
                            'fields': 'nextPageToken,files(id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                                          'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit))'}
        aiohttpretty.register_json_uri('GET', list_file_url, params=list_file_params, body=body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', item['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        item['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(item, path.child(item['name']))

        assert result == [expected]

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_folder_nested(self, provider, root_provider_fixtures):
        path = GoogleDriveInstitutionsPath(
            '/hugo/kim/pins/',
            _ids=[str(x) for x in range(4)]
        )

        body = generate_list(3, root_provider_fixtures)
        item = body['files'][0]

        query = provider._build_query(path.identifier)
        url = provider.build_url('files')
        params = {'q': query, 'alt':'json', 'pageSize': '1000',
                  'fields': 'nextPageToken,files(id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                                'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit))'}

        children_params = {'q': "'{}' in parents".format(path.identifier), 'fields': 'files(id)'}

        aiohttpretty.register_json_uri('GET', url, params=params, body=body)
        aiohttpretty.register_json_uri('GET', url, params=children_params, body={'files': []})

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', item['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        item['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(item, path.child(item['name']))

        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=url, params=params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_folder_metadata(self, provider, root_provider_fixtures):
        path = GoogleDriveInstitutionsPath(
            '/hugo/kim/pins/',
            _ids=[str(x) for x in range(4)]
        )

        body = generate_list(3, root_provider_fixtures, **root_provider_fixtures['folder_metadata'])
        item = body['files'][0]

        query = provider._build_query(path.identifier)
        url = provider.build_url('files')
        params = {'q': query, 'alt':'json', 'pageSize': '1000',
                  'fields': 'nextPageToken,files(id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                                'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit))'}

        aiohttpretty.register_json_uri('GET', url, params=params, body=body)

        result = await provider.metadata(path)

        expected = GoogleDriveInstitutionsFolderMetadata(item, path.child(item['name'], folder=True))

        assert result == [expected]
        assert aiohttpretty.has_call(method='GET', uri=url, params=params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        revisions_body = sharing_fixtures['canEdit_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = revisions_body['revisions'][-1]['id']
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url, params=revisions_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_gdoc_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = sharing_fixtures['canEdit_gdoc']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_GOOD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = revision_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.metadata(path, revision=self.GDOC_GOOD_REVISION)

        local_revision = copy.deepcopy(revision_body)
        local_revision['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileRevisionMetadata(local_revision, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revision_url, params=revision_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = make_no_such_revision_error(self.GDOC_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        revisions_body = sharing_fixtures['canEdit_gdoc']['revisions']
        revisions_url = provider.build_url('files', metadata_body['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params, body=revisions_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = revisions_body['revisions'][-1]['id']
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=revisions_url, params=revisions_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = local_metadata['modifiedTime'] + ds.DRIVE_IGNORE_VERSION
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.GDOC_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.GDOC_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_gdoc_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_gdoc']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_gdoc',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = metadata_body['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['version'] = local_metadata['modifiedTime'] + ds.DRIVE_IGNORE_VERSION
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_jpeg_good_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        revision_body = sharing_fixtures['canEdit_jpeg']['revision']
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_GOOD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, body=revision_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'], 'revisions', revision_body['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path, revision=self.JPEG_GOOD_REVISION)

        local_revision = copy.deepcopy(revision_body)
        local_revision['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileRevisionMetadata(local_revision, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=revision_url, params=revision_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        no_such_revision_error = make_no_such_revision_error(self.JPEG_BAD_REVISION)
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=no_such_revision_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_canEdit_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['canEdit_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/canEdit_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_no_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewaable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_bad_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        unauthorized_error = make_unauthorized_file_access_error(metadata_body['id'])
        revision_url = provider.build_url('files', metadata_body['id'],
                                          'revisions', self.JPEG_BAD_REVISION)
        revision_params = {'fields': 'id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size'}
        aiohttpretty.register_json_uri('GET', revision_url, params=revision_params, status=404, body=unauthorized_error)

        with pytest.raises(exceptions.NotFoundError) as e:
            await provider.metadata(path, revision=self.JPEG_BAD_REVISION)

        assert e.value.code == 404

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_metadata_viewable_jpeg_magic_revision(self, provider, sharing_fixtures):
        metadata_body = sharing_fixtures['viewable_jpeg']['metadata']
        path = GoogleDriveInstitutionsPath(
            '/sharing/viewable_jpeg.jpeg',
            _ids=['1', '2', metadata_body['id']]
        )

        metadata_url = provider.build_url('files', path.identifier)
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=metadata_body)

        file_content = b'we love you conrad'
        download_file_url = provider.build_url('files', metadata_body['id'])
        download_file_params = {'alt': 'media'}
        aiohttpretty.register_uri('GET', download_file_url, params=download_file_params, body=file_content, auto_length=True)

        result = await provider.metadata(path, revision=self.MAGIC_REVISION)

        local_metadata = copy.deepcopy(metadata_body)
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, path)

        assert result == expected
        assert aiohttpretty.has_call(method='GET', uri=metadata_url, params=metadata_params)
        assert aiohttpretty.has_call(method='GET', uri=download_file_url, params=download_file_params)


class TestRevisions:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions(self, provider, revision_fixtures, root_provider_fixtures):
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', item['id']))

        revisions_url = provider.build_url('files', item['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params,
                                       body=revision_fixtures['revisions_list'])

        result = await provider.revisions(path)
        expected = [
            GoogleDriveInstitutionsRevision(each)
            for each in revision_fixtures['revisions_list']['revisions']
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_no_revisions(self, provider, revision_fixtures,
                                              root_provider_fixtures):
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', item['id']))

        metadata_url = provider.build_url('files', item['id'])
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        revisions_url = provider.build_url('files', item['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}

        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=item)
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params,
                                       body=revision_fixtures['revisions_list_empty'])

        result = await provider.revisions(path)
        expected = [
            GoogleDriveInstitutionsRevision({
                'modifiedTime': item['modifiedTime'],
                'id': item['modifiedTime'] + ds.DRIVE_IGNORE_VERSION,
            })
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_for_no_canEdit(self, provider, sharing_fixtures):
        file_fixtures = sharing_fixtures['viewable_gdoc']
        item = file_fixtures['metadata']
        path = WaterButlerPath('/birdie.jpg', _ids=('doesntmatter', item['id']))

        metadata_url = provider.build_url('files', item['id'])
        metadata_params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}

        revisions_url = provider.build_url('files', item['id'], 'revisions')
        revisions_params = {'fields': 'revisions(id,mimeType,modifiedTime,exportLinks,originalFilename,md5Checksum,size)'}

        aiohttpretty.register_json_uri('GET', metadata_url, params=metadata_params, body=item)
        aiohttpretty.register_json_uri('GET', revisions_url, params=revisions_params, body=file_fixtures['revisions_error'], status=403)

        result = await provider.revisions(path)
        expected = [
            GoogleDriveInstitutionsRevision({
                'modifiedTime': item['modifiedTime'],
                'id': item['modifiedTime'] + ds.DRIVE_IGNORE_VERSION,
            })
        ]
        assert result == expected

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_get_revisions_doesnt_exist(self, provider):
        with pytest.raises(exceptions.NotFoundError):
            await provider.revisions(WaterButlerPath('/birdie.jpg'))


class TestCreateFolder:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_already_exists(self, provider):
        path = WaterButlerPath('/hugo/', _ids=('doesnt', 'matter'))

        with pytest.raises(exceptions.FolderNamingConflict) as e:
            await provider.create_folder(path)

        assert e.value.code == 409
        assert e.value.message == ('Cannot create folder "hugo", because a file or folder '
                                   'already exists with that name')

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_returns_metadata(self, provider, root_provider_fixtures):
        path = WaterButlerPath('/osf%20test/', _ids=(provider.folder['id'], None))

        url = provider.build_url('files')
        params = {
            'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                      'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'
        }
        aiohttpretty.register_json_uri('POST', url, params=params, body=root_provider_fixtures['folder_metadata'])

        resp = await provider.create_folder(path)

        assert resp.kind == 'folder'
        assert resp.name == 'osf test'
        assert resp.path == '/osf%20test/'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_raises_non_404(self, provider):
        path = WaterButlerPath('/hugo/kim/pins/', _ids=(provider.folder['id'],
                                                        'something', 'something', None))

        url = provider.build_url('files')
        params = {
            'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                      'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'
        }
        aiohttpretty.register_json_uri('POST', url, params=params, status=418)

        with pytest.raises(exceptions.CreateFolderError) as e:
            await provider.create_folder(path)

        assert e.value.code == 418

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_must_be_folder(self, provider, monkeypatch):
        with pytest.raises(exceptions.CreateFolderError):
            await provider.create_folder(WaterButlerPath('/carp.fish', _ids=('doesnt', 'matter')))


class TestIntraFunctions:

    GDOC_EXPORT_MIME_TYPE = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_file(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']
        src_path = WaterButlerPath('/unsure.txt', _ids=(provider.folder['id'], item['id']))
        dest_path = WaterButlerPath('/really/unsure.txt', _ids=(provider.folder['id'],
                                                                item['id'], item['id']))

        url = provider.build_url('files', src_path.identifier)
        params = {
            'addParents': dest_path.parent.identifier,
            'files': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)',
            'removeParents': src_path.parent.identifier
        }
        data = json.dumps({
            'name': dest_path.name
        })
        aiohttpretty.register_json_uri('PATCH', url, params=params, data=data, body=item)

        delete_url = provider.build_url('files', item['id'])
        aiohttpretty.register_uri('DELETE', delete_url, status=204)

        file_content = b'we love you conrad'
        download_file_url = item['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result, created = await provider.intra_move(provider, src_path, dest_path)

        local_metadata = copy.deepcopy(item)
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, dest_path)

        assert result == expected
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_move_folder(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['folder_metadata']
        src_path = WaterButlerPath('/unsure/', _ids=(provider.folder['id'], item['id']))
        dest_path = WaterButlerPath('/really/unsure/', _ids=(provider.folder['id'],
                                                             item['id'], item['id']))

        url = provider.build_url('files', src_path.identifier)
        params = {
            'addParents': dest_path.parent.identifier,
            'removeParents': src_path.parent.identifier,
            'files': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                     'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'
        }
        data = json.dumps({
            'name': dest_path.name
        })
        aiohttpretty.register_json_uri('PATCH', url, params=params, data=data, body=item)

        delete_url = provider.build_url('files', item['id'])
        aiohttpretty.register_uri('DELETE', delete_url, status=204)

        children_query = provider._build_query(dest_path.identifier)
        children_url = provider.build_url('files')
        children_params = {'q': children_query, 'alt': 'json', 'pageSize': '1000',
                           'fields': 'nextPageToken,files(id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                                                         'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit))'}
        children_list = generate_list(3, root_provider_fixtures,
                                      **root_provider_fixtures['folder_metadata'])
        aiohttpretty.register_json_uri('GET', children_url, params=children_params, body=children_list)

        result, created = await provider.intra_move(provider, src_path, dest_path)
        expected = GoogleDriveInstitutionsFolderMetadata(item, dest_path)
        expected.children = [
            await provider._serialize_item(dest_path.child(item['name']), item)
            for item in children_list['files']
        ]

        assert result == expected
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_intra_copy_file(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']
        src_path = WaterButlerPath('/unsure.txt', _ids=(provider.folder['id'], item['id']))
        dest_path = WaterButlerPath('/really/unsure.txt', _ids=(provider.folder['id'],
                                                                item['id'], item['id']))

        url = provider.build_url('files', src_path.identifier, 'copy')
        params = {'fields': 'id,name,version,size,modifiedTime,createdTime,mimeType,webViewLink,' \
                            'originalFilename,md5Checksum,exportLinks,ownedByMe,capabilities(canEdit)'}
        data = json.dumps({
            'parents': [dest_path.parent.identifier],
            'name': dest_path.name
        }),
        aiohttpretty.register_json_uri('POST', url, params=params, data=data, body=item)

        delete_url = provider.build_url('files', item['id'])
        aiohttpretty.register_uri('DELETE', delete_url, status=204)

        file_content = b'we love you conrad'
        download_file_url = item['exportLinks'][self.GDOC_EXPORT_MIME_TYPE]
        aiohttpretty.register_uri('GET', download_file_url, body=file_content, auto_length=True)

        result, created = await provider.intra_copy(provider, src_path, dest_path)

        local_metadata = copy.deepcopy(item)
        local_metadata['sha512'] = hashlib.sha512(file_content).hexdigest()
        expected = GoogleDriveInstitutionsFileMetadata(local_metadata, dest_path)

        assert result == expected
        assert aiohttpretty.has_call(method='DELETE', uri=delete_url)


class TestOperationsOrMisc:

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_can_duplicate_names(self, provider):
        assert provider.can_duplicate_names() is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_shares_storage_root(self, provider, other_provider):
        assert provider.shares_storage_root(other_provider) is True
        assert provider.shares_storage_root(provider) is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_can_intra_move(self, provider, other_provider):
        assert provider.can_intra_move(other_provider) is False
        assert provider.can_intra_move(provider) is True

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test__serialize_item_raw(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']

        assert await provider._serialize_item(None, item, True) == item

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_can_intra_copy(self, provider, other_provider, root_provider_fixtures):
        item = root_provider_fixtures['list_file']['files'][0]
        path = WaterButlerPath('/birdie.jpg', _ids=(provider.folder['id'], item['id']))

        assert provider.can_intra_copy(other_provider, path) is False
        assert provider.can_intra_copy(provider, path) is True

    def test_path_from_metadata(self, provider, root_provider_fixtures):
        item = root_provider_fixtures['docs_file_metadata']
        src_path = WaterButlerPath('/version-test.docx', _ids=(provider.folder['id'], item['id']))

        metadata = GoogleDriveInstitutionsFileMetadata(item, src_path)
        child_path = provider.path_from_metadata(src_path.parent, metadata)

        assert child_path.full_path == src_path.full_path
        assert child_path == src_path

    @pytest.mark.asyncio
    @pytest.mark.aiohttpretty
    async def test_revalidate_path_file_error(self, provider, root_provider_fixtures,
                                              error_fixtures):
        file_name = '/root/whatever/Gear1.stl'
        file_id = root_provider_fixtures['revalidate_path_file_metadata_1']['files'][0]['id']
        path = GoogleDriveInstitutionsPath(file_name, _ids=['0', file_id, file_id, file_id])

        parts = [[parse.unquote(x), True] for x in file_name.strip('/').split('/')]
        parts[-1][1] = False
        current_part = parts.pop(0)
        part_name, part_is_folder = current_part[0], current_part[1]
        query = _build_name_search_query(provider, part_name, provider.folder['id'], True)

        url = provider.build_url('files')
        params = {'q': query, 'fields': 'files(id)'}
        aiohttpretty.register_json_uri('GET', url, params=params,
                                       body=error_fixtures['parts_file_missing_metadata'])

        with pytest.raises(exceptions.MetadataError) as e:
            _ = await provider._resolve_path_to_ids(file_name)

        assert e.value.message == '{} not found'.format(str(path))
        assert e.value.code == 404
