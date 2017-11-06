// Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
import fetchMock from '../utils/fetch-mock';
import executionAfm, { nextPageOffset, mergePageData } from '../../src/execution/execute-afm';

describe('nextPageOffset', () => {
    it('should work for 1 dimension', () => {
        expect(nextPageOffset({ offset: [0], total: [501] })).toEqual([500]);
        expect(nextPageOffset({ offset: [500], total: [501] })).toEqual(false);
        expect(nextPageOffset({ offset: [0], total: [1] })).toEqual(false);
    });
    it('should work for 2 dimensions', () => {
        expect(nextPageOffset({ offset: [0, 0], total: [501, 501] })).toEqual([0, 500]);
        expect(nextPageOffset({ offset: [0, 500], total: [501, 501] })).toEqual([500, 0]);
        expect(nextPageOffset({ offset: [500, 0], total: [501, 501] })).toEqual([500, 500]);
        expect(nextPageOffset({ offset: [500, 500], total: [501, 501] })).toEqual(false);
    });
    it('should work for 3 dimensions', () => {
        expect(nextPageOffset({ offset: [0, 0, 0], total: [501, 501, 501] })).toEqual([0, 0, 500]);
        expect(nextPageOffset({ offset: [0, 0, 0], total: [501, 501, 501] })).toEqual([0, 0, 500]);
        expect(nextPageOffset({ offset: [500, 500, 500], total: [501, 501, 501] })).toEqual(false);
    });
});

describe('mergePageData', () => {
    it('should work for 1 dimension', () => {
        let result = { data: [1] };

        result = mergePageData(result, { paging: { offset: [1] }, data: [2] });
        expect(result).toEqual({ data: [1, 2] });

        result = mergePageData(result, { paging: { offset: [2] }, data: [3] });
        expect(result).toEqual({ data: [1, 2, 3] });
    });

    it('should work for 2 dimensions', () => {
        let result = { data: [[11, 12], [21, 22]] };

        result = mergePageData(result, { paging: { offset: [0, 2] }, data: [[13], [23]] });
        expect(result).toEqual({ data: [[11, 12, 13], [21, 22, 23]] });

        result = mergePageData(result, { paging: { offset: [2, 0] }, data: [[51, 52]] });
        result = mergePageData(result, { paging: { offset: [2, 2] }, data: [[53]] });
        expect(result).toEqual({ data: [[11, 12, 13], [21, 22, 23], [51, 52, 53]] });
    });
});

describe('executionAfm', () => {
    beforeEach(() => {
        expect.hasAssertions();
        fetchMock.restore();
    });

    function pollingResponseBody() {
        return {
            executionResponse: {
                dimensions: [],
                links: {
                    executionResult: '/gdc/app/projects/myFakeProjectId/executionResults/123'
                }
            }
        };
    }

    function executionResultResponseBody() {
        return { executionResult: { data: [[11, 12], [51, 52]], paging: { total: [2, 2], offset: [0, 0] } } };
    }

    it('should reject when executeAfm fails', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            400
        );
        return executionAfm('myFakeProjectId', {}).catch((err) => {
            expect(err).toBeInstanceOf(Error);
            expect(err.response.status).toBe(400);
        });
    });

    it('should reject when first polling fails', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=0%2C0',
            400
        );
        return executionAfm('myFakeProjectId', {}).catch((err) => {
            expect(err).toBeInstanceOf(Error);
            expect(err.response.status).toBe(400);
        });
    });

    it('should reject when first polling returns 204', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=0%2C0',
            204
        );
        return executionAfm('myFakeProjectId', {}).catch((err) => {
            expect(err).toBeInstanceOf(Error);
            expect(err.response.status).toBe(204);
        });
    });

    it('should reject when first polling returns 413', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=0%2C0',
            413
        );
        return executionAfm('myFakeProjectId', {}).catch((err) => {
            expect(err).toBeInstanceOf(Error);
            expect(err.response.status).toBe(413);
        });
    });

    it('should resolve on first polling', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=0%2C0',
            { status: 200, body: JSON.stringify(executionResultResponseBody()) }
        );
        return executionAfm('myFakeProjectId', {}).then((response) => {
            expect(response).toEqual({
                executionResponse: {
                    dimensions: [],
                    links: {
                        executionResult: '/gdc/app/projects/myFakeProjectId/executionResults/123'
                    }
                },
                ...executionResultResponseBody()
            });
        });
    });

    it('should resolve on second polling', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );
        let pollingCounter = 0;
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=0%2C0',
            () => {
                pollingCounter += 1;
                return pollingCounter === 1
                    ? {
                        status: 202,
                        headers: { Location: '/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=0%2C0' } //eslint-disable-line
                    }
                    : {
                        status: 200,
                        body: JSON.stringify(executionResultResponseBody())
                    };
            }
        );
        return executionAfm('myFakeProjectId', {}).then((response) => {
            expect(response).toEqual({
                executionResponse: {
                    dimensions: [],
                    links: {
                        executionResult: '/gdc/app/projects/myFakeProjectId/executionResults/123'
                    }
                },
                ...executionResultResponseBody()
            });
        });
    });

    it('should resolve with 2x2 pages', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );

        const pagesByOffset = {
            '0,0': { executionResult: { data: Array(500).fill([1, 2]), paging: { total: [501, 501], offset: [0, 0] } } },
            '0,500': { executionResult: { data: Array(500).fill([3]), paging: { total: [501, 501], offset: [0, 500] } } },
            '500,0': { executionResult: { data: [[91, 92]], paging: { total: [501, 501], offset: [500, 0] } } },
            '500,500': { executionResult: { data: [[93]], paging: { total: [501, 501], offset: [500, 500] } } }
        };

        fetchMock.mock(
            'begin:/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500%2C500&offset=',
            (url) => {
                const offset = url.replace(/.*offset=/, '').replace('%2C', ',');
                return { status: 200, body: JSON.stringify(pagesByOffset[offset]) };
            }
        );
        return executionAfm('myFakeProjectId', {}).then((response) => {
            expect(response).toEqual({
                executionResponse: {
                    dimensions: [],
                    links: {
                        executionResult: '/gdc/app/projects/myFakeProjectId/executionResults/123'
                    }
                },
                executionResult: {
                    data: [...Array(500).fill([1, 2, 3]), [91, 92, 93]],
                    paging: {
                        total: [501, 501],
                        offset: [0, 0]
                    }
                }
            });
        });
    });

    it('should resolve for 1 dimension x 2 pages', () => {
        fetchMock.mock(
            '/gdc/app/projects/myFakeProjectId/executeAfm',
            { status: 200, body: JSON.stringify(pollingResponseBody()) }
        );

        const pagesByOffset = {
            0: { executionResult: { data: [1], paging: { total: [501], offset: [0] } } },
            500: { executionResult: { data: [2], paging: { total: [501], offset: [500] } } }
        };

        fetchMock.mock(
            'begin:/gdc/app/projects/myFakeProjectId/executionResults/123?limit=500&offset=',
            (url) => {
                const offset = url.replace(/.*offset=/, '').replace('%2C', ',');
                return { status: 200, body: JSON.stringify(pagesByOffset[offset]) };
            }
        );
        return executionAfm('myFakeProjectId', { execution: { resultSpec: { dimensions: [1] } } }).then((response) => {
            expect(response).toEqual({
                executionResponse: {
                    dimensions: [],
                    links: {
                        executionResult: '/gdc/app/projects/myFakeProjectId/executionResults/123'
                    }
                },
                executionResult: {
                    data: [1, 2],
                    paging: {
                        total: [501],
                        offset: [0]
                    }
                }
            });
        });
    });
});
