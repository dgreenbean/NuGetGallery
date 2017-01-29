// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;

namespace NuGetGallery
{
    public class NullAggregateStatsService : IAggregateStatsService
    {
        [SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes", Justification = "Type is immutable")]
        public static readonly NullAggregateStatsService Instance = new NullAggregateStatsService();

        private NullAggregateStatsService() { }

        public Task<AggregateStats> GetAggregateStats()
        {
            return Task.FromResult<AggregateStats>(null);
        }
    }
}