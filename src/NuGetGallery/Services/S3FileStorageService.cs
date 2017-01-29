// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using System.Web.Mvc;
using NuGetGallery.Configuration;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;

namespace NuGetGallery
{
    public class S3FileStorageService : IFileStorageService
    {
        private readonly IAppConfiguration _configuration;

        public S3FileStorageService(IAppConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<ActionResult> CreateDownloadFileActionResultAsync(Uri requestUrl, string folderName, string fileName)
        {
            if (String.IsNullOrWhiteSpace(folderName))
            {
                throw new ArgumentNullException(nameof(folderName));
            }

            if (String.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            var path = BuildPath(_configuration.S3Prefix, folderName, fileName);

            using (AmazonS3Client s3Client = CreateClient())
            using (GetObjectResponse response = await s3Client.GetObjectAsync(new GetObjectRequest { BucketName = _configuration.S3Bucket, Key = path }))
            {
                return new FileStreamResult(response.ResponseStream, response.Headers.ContentType);
            }
        }

        public async Task DeleteFileAsync(string folderName, string fileName)
        {
            if (String.IsNullOrWhiteSpace(folderName))
            {
                throw new ArgumentNullException(nameof(folderName));
            }

            if (String.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            var path = BuildPath(_configuration.S3Prefix, folderName, fileName);

            using (AmazonS3Client s3Client = CreateClient())
            {
                await s3Client.DeleteObjectAsync(new DeleteObjectRequest { BucketName = _configuration.S3Bucket, Key = path });
            }
        }

        public async Task<bool> FileExistsAsync(string folderName, string fileName)
        {
            if (String.IsNullOrWhiteSpace(folderName))
            {
                throw new ArgumentNullException(nameof(folderName));
            }

            if (String.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            var path = BuildPath(_configuration.S3Prefix, folderName, fileName);

            using (AmazonS3Client s3Client = CreateClient())
            {
                try
                {
                    GetObjectMetadataResponse response = await s3Client.GetObjectMetadataAsync(new GetObjectMetadataRequest { BucketName = _configuration.S3Bucket, Key = path });
                } catch (AmazonS3Exception exception)
                {
                    if (exception.StatusCode == System.Net.HttpStatusCode.NotFound)
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        public async Task<Stream> GetFileAsync(string folderName, string fileName)
        {
            if (String.IsNullOrWhiteSpace(folderName))
            {
                throw new ArgumentNullException(nameof(folderName));
            }

            if (String.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            var path = BuildPath(_configuration.S3Prefix, folderName, fileName);

            using (AmazonS3Client s3Client = CreateClient())
            using (GetObjectResponse response = await s3Client.GetObjectAsync(new GetObjectRequest { BucketName = _configuration.S3Bucket, Key = path }))
            {
                return response.ResponseStream;
            }
        }

        public async Task<IFileReference> GetFileReferenceAsync(string folderName, string fileName, string ifNoneMatch = null)
        {
            if (String.IsNullOrWhiteSpace(folderName))
            {
                throw new ArgumentNullException(nameof(folderName));
            }

            if (String.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }


            var path = BuildPath(_configuration.S3Prefix, folderName, fileName);

            using (AmazonS3Client s3Client = CreateClient())
            using (GetObjectResponse response = await s3Client.GetObjectAsync(new GetObjectRequest { BucketName = _configuration.S3Bucket, Key = path }))
            {
                if (response.HttpStatusCode == System.Net.HttpStatusCode.NotModified)
                {
                    return S3FileReference.NotModified(ifNoneMatch);
                }
                else if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
                {
                    return S3FileReference.Modified(response);
                }
                else
                {
                    // Not found
                    return null;
                }
            }
        }

        public async Task SaveFileAsync(string folderName, string fileName, Stream packageFile, bool overwrite = true)
        {
            if (String.IsNullOrWhiteSpace(folderName))
            {
                throw new ArgumentNullException(nameof(folderName));
            }

            if (String.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentNullException(nameof(fileName));
            }

            if (packageFile == null)
            {
                throw new ArgumentNullException(nameof(packageFile));
            }

            var path = BuildPath(_configuration.S3Prefix, folderName, fileName);

            string contentType = GetContentType(folderName);

            using (AmazonS3Client s3Client = CreateClient())
            {
                PutObjectResponse response = await s3Client.PutObjectAsync(new PutObjectRequest { BucketName = _configuration.S3Bucket, Key = path, InputStream = packageFile });
            }
        }

        public Task<bool> IsAvailableAsync()
        {
            return Task.FromResult(true);
        }

        private AmazonS3Client CreateClient()
        {
            if (!string.IsNullOrEmpty(_configuration.AwsAccessKeyId) && !string.IsNullOrEmpty(_configuration.AwsSecretAccessKey) && !string.IsNullOrEmpty(_configuration.S3Region))
            {
                return new AmazonS3Client(_configuration.AwsAccessKeyId, _configuration.AwsSecretAccessKey, RegionEndpoint.GetBySystemName(_configuration.S3Region));
            }
            if (!string.IsNullOrEmpty(_configuration.AwsAccessKeyId) && !string.IsNullOrEmpty(_configuration.AwsSecretAccessKey))
            {
                return new AmazonS3Client(_configuration.AwsAccessKeyId, _configuration.AwsSecretAccessKey);
            }
            if (!string.IsNullOrEmpty(_configuration.S3Region))
            {
                return new AmazonS3Client(RegionEndpoint.GetBySystemName(_configuration.S3Region));
            }
            return new AmazonS3Client();
        }

        private static string GetContentType(string folderName)
        {
            switch (folderName)
            {
                case Constants.PackagesFolderName:
                case Constants.PackageBackupsFolderName:
                case Constants.UploadsFolderName:
                    return Constants.PackageContentType;

                case Constants.DownloadsFolderName:
                    return Constants.OctetStreamContentType;

                default:
                    throw new InvalidOperationException(
                        String.Format(CultureInfo.CurrentCulture, "The folder name {0} is not supported.", folderName));
            }
        }

        private static string BuildPath(string basePath, string folderName, string fileName)
        {
            if (string.IsNullOrEmpty(basePath))
            {
                return Path.Combine(folderName, fileName);
            }
            return Path.Combine(basePath, folderName, fileName);
        }
    }
}