// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NuGetGallery
{
    public class S3FileReference : IFileReference
    {
        private Stream _stream;
        private string _contentId;

        public string ContentId
        {
            get { return _contentId; }
        }

        private S3FileReference(Stream stream, string contentId)
        {
            _contentId = contentId;
            _stream = stream;
        }

        public Stream OpenRead()
        {
            return _stream;
        }

        public static S3FileReference NotModified(string contentId)
        {
            return new S3FileReference(null, contentId);
        }

        public static S3FileReference Modified(Amazon.S3.Model.GetObjectResponse response)
        {
            return new S3FileReference(response.ResponseStream, response.ETag);
        }
    }
}
