import { rejects, strictEqual } from 'node:assert'
import { describe, it } from 'mocha'
import nock from 'nock'
import rdf from 'rdf-ext'
import { datasetEqual } from 'rdf-test/assert.js'
import eventToPromise from 'rdf-utils-stream/eventToPromise.js'
import chunks from 'stream-chunks/chunks.js'
import WebStore from '../index.js'
import * as ns from './support/namespaces.js'

function prepareUrl (path) {
  const origin = 'http://example.org'
  const graph = rdf.namedNode(`${origin}${path}`)

  return {
    graph,
    origin,
    path
  }
}

describe('rdf-store-web', () => {
  const simpleDataset = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object, ns.ex.graph)])
  const simpleGraph = rdf.dataset(simpleDataset, rdf.defaultGraph())
  const simpleGraphNT = simpleGraph.toCanonical()

  describe('.match', () => {
    it('should use http GET method', async () => {
      const { graph, origin, path } = prepareUrl('/get-method')

      const scope = nock(origin)
        .get(path)
        .reply(200, simpleGraphNT, { 'Content-Type': 'text/turtle' })

      const store = new WebStore()

      const stream = store.match(null, null, null, graph)
      await chunks(stream)

      strictEqual(scope.isDone(), true)
    })

    it('should build accept header', async () => {
      const { graph, origin, path } = prepareUrl('/accept-header')

      const scope = nock(origin, {
        reqheaders: {
          accept: [...rdf.formats.parsers.keys()].join(', ')
        }
      })
        .get(path)
        .reply(200, simpleGraphNT, { 'Content-Type': 'text/turtle' })

      const store = new WebStore()

      await chunks(store.match(null, null, null, graph))

      strictEqual(scope.isDone(), true)
    })

    it('should return all matching quads', async () => {
      const { graph, origin, path } = prepareUrl('/quads')
      const expected = rdf.dataset(simpleGraph, graph)

      nock(origin)
        .get(path)
        .reply(200, simpleGraphNT, { 'Content-Type': 'text/turtle' })

      const store = new WebStore()

      const stream = store.match(null, null, null, graph)
      const result = await rdf.dataset().import(stream)

      datasetEqual(expected, result)
    })

    it('should handle request error', async () => {
      const { graph } = prepareUrl('/client-error')

      const store = new WebStore({
        fetch: async () => {
          throw new Error('error')
        }
      })

      await rejects(async () => {
        await chunks(store.match(null, null, null, graph))
      })
    })

    it('should handle error status code', async () => {
      const { graph, origin, path } = prepareUrl('/status-error')

      nock(origin)
        .get(path)
        .reply(500)

      const store = new WebStore()

      await rejects(async () => {
        await chunks(store.match(null, null, null, graph))
      })
    })

    it('should handle parser errors', async () => {
      const { graph, origin, path } = prepareUrl('/parser-error')

      nock(origin)
        .get(path)
        .reply(200, '1' + simpleGraphNT, { 'content-type': 'text/turtle' })

      const store = new WebStore()

      await rejects(async () => {
        await chunks(store.match(null, null, null, graph))
      })
    })

    it('should use the graph IRI as base IRI', async () => {
      const { graph, origin, path } = prepareUrl('/base-iri')
      const expected = rdf.dataset(simpleGraph, graph)

      nock(origin)
        .get(path)
        .reply(200, '<subject> <predicate> <object>.', { 'content-type': 'text/turtle' })

      const store = new WebStore()

      const stream = store.match(null, null, null, graph)
      const result = await rdf.dataset().import(stream)

      datasetEqual(expected, result)
    })
  })

  describe('.import', () => {
    it('should use http POST method', async () => {
      const { graph, origin, path } = prepareUrl('/post-method')

      const scope = nock(origin)
        .post(path)
        .reply(200)

      const store = new WebStore()

      const dataset = rdf.dataset(simpleGraph, graph)
      await eventToPromise(store.import(dataset.toStream()))

      strictEqual(scope.isDone(), true)
    })

    it('should send the given dataset', async () => {
      const { graph, origin, path } = prepareUrl('/send-dataset')
      const expected = rdf.dataset(simpleGraph, graph)

      let result

      nock(origin)
        .post(path)
        .reply(async function (url, body, callback) {
          const mediaType = this.req.headers['content-type']
          result = await rdf.io.dataset.fromText(mediaType, body)

          callback(null, [201])
        })

      const store = new WebStore()

      await eventToPromise(store.import(expected.toStream()))

      datasetEqual(result, expected)
    })

    it('should use http PUT method to truncate graph', async () => {
      const { graph, origin, path } = prepareUrl('/put-method')

      const scope = nock(origin)
        .put(path)
        .reply(200)

      const store = new WebStore()

      const dataset = rdf.dataset(simpleGraph, graph)
      await eventToPromise(store.import(dataset.toStream(), { truncate: true }))

      strictEqual(scope.isDone(), true)
    })

    it('should do nothing if the graph is empty', async () => {
      const store = new WebStore()

      const dataset = rdf.dataset()

      await eventToPromise(store.import(dataset.toStream()))
    })

    it('should handle request error', async () => {
      const { graph } = prepareUrl('/import-client-error')

      const store = new WebStore({
        fetch: async () => {
          throw new Error('error')
        }
      })

      const dataset = rdf.dataset(simpleGraph, graph)

      await rejects(async () => {
        await chunks(store.import(dataset.toStream()))
      })
    })

    it('should handle error status code', async () => {
      const { graph, origin, path } = prepareUrl('/import-status-error')

      nock(origin)
        .post(path)
        .reply(500)

      const store = new WebStore()

      const dataset = rdf.dataset(simpleGraph, graph)

      await rejects(async () => {
        await chunks(store.import(dataset.toStream()))
      })
    })
  })

  describe('.remove', () => {
    it('should fetch and replace', async () => {
      const { graph, origin, path } = prepareUrl('/remove-fetch-replace')
      const input = rdf.dataset([
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object1),
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object2)
      ])
      const expected = rdf.dataset(input.match(null, null, ns.ex.object2), graph)
      const remove = rdf.dataset(input.match(null, null, ns.ex.object1), graph)

      let result

      nock(origin)
        .get(path)
        .reply(200, input.toCanonical(), { 'content-type': 'text/turtle' })

      nock(origin)
        .put(path)
        .reply(async function (url, body, callback) {
          const mediaType = this.req.headers['content-type']
          result = await rdf.io.dataset.fromText(mediaType, body)

          callback(null, [201])
        })

      const store = new WebStore()

      await eventToPromise(store.remove(remove.toStream()))

      datasetEqual(expected, result)
    })

    it('should do nothing if stream contains no quads', async () => {
      const remove = rdf.dataset()

      const store = new WebStore()

      await eventToPromise(store.remove(remove.toStream()))
    })

    it('should not send replace if no quads have been removed', async () => {
      const { graph, origin, path } = prepareUrl('/remove-no-changes')
      const input = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object1)])
      const remove = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object2)], graph)

      nock(origin)
        .get(path)
        .reply(200, input.toString(), { 'content-type': 'text/turtle' })

      const store = new WebStore()

      await eventToPromise(store.remove(remove.toStream()))
    })

    it('should handle fetch errors', async () => {
      const { graph, origin, path } = prepareUrl('/remove-fetch-error')
      const remove = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object)], graph)

      nock(origin)
        .get(path)
        .reply(500)

      nock(origin)
        .put(path)
        .reply(200, simpleGraphNT, { 'Content-Type': 'text/turtle' })

      const store = new WebStore()

      await rejects(async () => {
        await eventToPromise(store.remove(remove.toStream()))
      })
    })

    it('should handle replace errors', async () => {
      const { graph, origin, path } = prepareUrl('/remove-replace-error')
      const remove = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object)], graph)

      nock(origin)
        .get(path)
        .reply(200, simpleGraphNT, { 'content-type': 'text/turtle' })

      nock(origin)
        .put(path)
        .reply(500)

      const store = new WebStore()

      await rejects(async () => {
        await chunks(store.remove(remove.toStream()))
      })
    })
  })

  describe('.removeMatches', () => {
    it('should fetch and replace', async () => {
      const { graph, origin, path } = prepareUrl('/remove-matches-fetch-replace')
      const input = rdf.dataset([
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object1),
        rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object2)
      ])
      const expected = rdf.dataset(input.match(null, null, ns.ex.object2), graph)

      let result

      nock(origin)
        .get(path)
        .reply(200, input.toCanonical(), { 'content-type': 'text/turtle' })

      nock(origin)
        .put(path)
        .reply(async function (url, body, callback) {
          const mediaType = this.req.headers['content-type']
          result = await rdf.io.dataset.fromText(mediaType, body)

          callback(null, [201])
        })

      const store = new WebStore()

      await eventToPromise(store.removeMatches(null, null, ns.ex.object1, graph))

      datasetEqual(expected, result)
    })

    it('should not send replace if no quads have been removed', async () => {
      const { graph, origin, path } = prepareUrl('/remove-matches-no-changes')
      const input = rdf.dataset([rdf.quad(ns.ex.subject, ns.ex.predicate, ns.ex.object1)])

      nock(origin)
        .get(path)
        .reply(200, input.toCanonical(), { 'content-type': 'text/turtle' })

      const store = new WebStore()

      await eventToPromise(store.removeMatches(null, null, ns.ex.object2, graph))
    })

    it('should handle fetch errors', async () => {
      const { graph, origin, path } = prepareUrl('/remove-matches-fetch-error')

      nock(origin)
        .get(path)
        .reply(500)

      nock(origin)
        .put(path)
        .reply(200, simpleGraphNT, { 'Content-Type': 'text/turtle' })

      const store = new WebStore()

      await rejects(async () => {
        await chunks(store.removeMatches(null, null, ns.ex.object, graph))
      })
    })

    it('should handle replace errors', async () => {
      const { graph, origin, path } = prepareUrl('/remove-matches-replace-error')

      nock(origin)
        .get(path)
        .reply(200, simpleGraphNT, { 'content-type': 'text/turtle' })

      nock(origin)
        .put(path)
        .reply(500)

      const store = new WebStore()

      await rejects(async () => {
        await chunks(store.removeMatches(null, null, ns.ex.object, graph))
      })
    })
  })

  describe('.deleteGraph', () => {
    it('should use http DELETE method', async () => {
      const { graph, origin, path } = prepareUrl('/delete-method')

      const scope = nock(origin)
        .delete(path)
        .reply(201)

      const store = new WebStore()

      await eventToPromise(store.deleteGraph(graph))

      strictEqual(scope.isDone(), true)
    })

    it('should handle request error', async () => {
      const { graph } = prepareUrl('/delete-graph-client-error')

      const store = new WebStore({
        fetch: () => {
          throw new Error('error')
        }
      })

      await rejects(async () => {
        await eventToPromise(store.deleteGraph(graph))
      })
    })

    it('should handle error status code', async () => {
      const { graph, origin, path } = prepareUrl('/delete-graph-status-error')

      nock(origin)
        .delete(path)
        .reply(500)

      const store = new WebStore()

      await rejects(async () => {
        await eventToPromise(store.deleteGraph(graph))
      })
    })
  })
})
