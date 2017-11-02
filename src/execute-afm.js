// Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
import { get, mapValues, clone } from 'lodash';
import invariant from 'invariant';
import { ajax, post, parseJSON } from './xhr';
import { queryString } from './util';

const PAGE_SIZE = 500;
const DEFAULT_DIMENSION_COUNT = 2;

function getDimensionality(execution) {
    return get(execution, 'execution.resultSpec.dimensions').length || DEFAULT_DIMENSION_COUNT;
}

function getLimit(offset) {
    return Array(offset.length).fill(PAGE_SIZE);
}

function fetchExecutionResult(pollingUri, offset) {
    const query = { limit: getLimit(offset), offset };
    const uri = pollingUri + queryString(mapValues(query, arr => arr.join(',')));
    return ajax(uri, { method: 'GET' }).then((r) => {
        if (r.status === 204) {
            const err = new Error('Loading executeAfm failed: 204 No Content');
            err.response = r;
            throw err;
        }
        return r.json();
    });
}

// works only for one or two dimensions
export function mergePageData(resultSoFar, { offset, data }) {
    const rowOffset = offset[0];
    if (resultSoFar.data[rowOffset]) { // appending columns to existing rows
        for (let i = 0; i < data.length; i += 1) {
            resultSoFar.data[i + rowOffset].push(...data[i]);
        }
    } else { // appending new rows
        resultSoFar.data.push(...data);
    }
    return resultSoFar;
}

export function nextPageOffset({ offset, overallSize }) {
    const newOffset = clone(offset);
    const maxDimension = offset.length - 1;
    // we need last dimension first (aka columns, then rows) to allow array appending in merge fnc
    for (let i = maxDimension; i >= 0; i -= 1) {
        if (newOffset[i] + PAGE_SIZE < overallSize[i]) {
            newOffset[i] += PAGE_SIZE;
            return newOffset;
        }
        newOffset[i] = 0;
    }
    return false;
}

function getOnePage(pollingUri, offset, resultSoFar = false) {
    return fetchExecutionResult(pollingUri, offset).then(({ executionResult }) => {
        const newResult = resultSoFar ? mergePageData(resultSoFar, executionResult) : executionResult;

        const nextOffset = nextPageOffset(executionResult);
        return nextOffset
            ? getOnePage(pollingUri, nextOffset, newResult)
            : newResult;
    });
}

export default function executeAfm(projectId, execution) {
    const dimensionality = getDimensionality(execution);
    invariant(dimensionality <= 2, 'executeAfm does not support more than 2 dimensions');

    return post(`/gdc/app/projects/${projectId}/executeAfm`, { body: JSON.stringify(execution) })
        .then(parseJSON)
        .then(({ executionResponse }) => {
            const offset = Array(dimensionality).fill(0); // offset holds information on dimensionality
            return getOnePage(executionResponse.links.executionResult, offset).then((executionResult) => {
                return {
                    executionResponse,
                    executionResult
                };
            });
        });
}
