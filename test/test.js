/* global describe, it */

const assert = require('assert')
const formats = require('rdf-formats-common')()
const nock = require('nock')
const rdf = require('rdf-ext')
const rdfFetch = require('rdf-fetch')
const stringToStream = require('string-to-stream')
const WebStore = require('..')

function expectError (p) {
  return new Promise((resolve, reject) => {
    Promise.resolve().then(() => {
      return p()
    }).then(() => {
      reject(new Error('no error thrown'))
    }).catch(() => {
      resolve()
    })
  })
}

describe('rdf-store-web', () => {
  const simpleDataset = rdf.dataset([
    rdf.quad(
      rdf.namedNode('http://example.org/subject'),
      rdf.namedNode('http://example.org/predicate'),
      rdf.literal('object'),
      rdf.namedNode('http://example.org/graph')
    )
  ])

  const simpleGraph = rdf.graph(simpleDataset)

  const simpleGraphNT = '<http://example.org/subject> <http://example.org/predicate> "object".'

  describe('.match', () => {
    it('should use http GET method', () => {
      const urlPath = '/get-method'

      nock('http://example.org')
        .get(urlPath)
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath)))
    })

    it('should build accept header', () => {
      const urlPath = '/accept-header'

      nock('http://example.org', {
        reqheaders: {
          accept: rdfFetch.defaults.formats.parsers.list().join(', ')
        }
      })
        .get(urlPath)
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath)))
    })

    it('should return all matching quads', () => {
      const urlPath = '/quads'

      nock('http://example.org')
        .get(urlPath)
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath))).then((dataset) => {
        assert(rdf.dataset(simpleGraph, rdf.namedNode('http://example.org' + urlPath)).equals(dataset))
      })
    })

    it('should handle request error', () => {
      const urlPath = '/client-error'

      const options = {
        fetch: () => {
          return Promise.reject(new Error('error'))
        }
      }

      let store = new WebStore(options)

      return expectError(() => {
        return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath)))
      })
    })

    it('should handle error status code', () => {
      const urlPath = '/status-error'

      nock('http://example.org')
        .get(urlPath)
        .reply(500)

      let store = new WebStore()

      return expectError(() => {
        return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath)))
      })
    })

    it('should handle parser errors', () => {
      const urlPath = '/parser-error'

      nock('http://example.org')
        .get(urlPath)
        .reply(200, '1' + simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return expectError(() => {
        return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath)))
      })
    })

    it('should use the graph IRI as base IRI', () => {
      const urlPath = '/base-iri'

      nock('http://example.org')
        .get(urlPath)
        .reply(200, '<subject> <predicate> "object".', {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return rdf.dataset().import(store.match(null, null, null, rdf.namedNode('http://example.org' + urlPath))).then((dataset) => {
        assert(rdf.dataset(simpleGraph, rdf.namedNode('http://example.org' + urlPath)).equals(dataset))
      })
    })
  })

  describe('.import', () => {
    it('should use http POST method', () => {
      const urlPath = '/post-method'

      let sentDataset

      nock('http://example.org')
        .post(urlPath)
        .reply(200, function (url, body) {
          let mediaType = this.req.headers['content-type'][0]

          rdf.dataset().import(formats.parsers.import(mediaType, stringToStream(body))).then((dataset) => {
            sentDataset = dataset
          })

          return [simpleGraphNT, {'Content-Type': 'text/turtle'}]
        })

      let store = new WebStore()

      let dataset = rdf.dataset(simpleGraph, rdf.namedNode('http://example.org' + urlPath))

      return rdf.waitFor(store.import(dataset.toStream())).then(() => {
        assert(simpleGraph.equals(sentDataset))
      })
    })

    it('should use http PUT method to truncate graph', () => {
      const urlPath = '/put-method'

      let sentDataset

      nock('http://example.org')
        .put(urlPath)
        .reply(200, function (url, body) {
          let mediaType = this.req.headers['content-type'][0]

          rdf.dataset().import(formats.parsers.import(mediaType, stringToStream(body))).then((dataset) => {
            sentDataset = dataset
          })

          return [simpleGraphNT, {'Content-Type': 'text/turtle'}]
        })

      let store = new WebStore()

      let dataset = rdf.dataset(simpleGraph, rdf.namedNode('http://example.org' + urlPath))

      return rdf.waitFor(store.import(dataset.toStream(), {truncate: true})).then(() => {
        assert(simpleGraph.equals(sentDataset))
      })
    })

    it('should do nothing if the graph is empty', () => {
      let store = new WebStore()

      let dataset = rdf.dataset()

      return rdf.waitFor(store.import(dataset.toStream()))
    })

    it('should handle request error', () => {
      const urlPath = '/import-client-error'

      const options = {
        fetch: () => {
          return Promise.reject(new Error('error'))
        }
      }

      let store = new WebStore(options)

      let dataset = rdf.dataset(simpleGraph, rdf.namedNode('http://example.org' + urlPath))

      return expectError(() => {
        return rdf.waitFor(store.import(dataset.toStream()))
      })
    })

    it('should handle error status code', () => {
      const urlPath = '/import-status-error'

      nock('http://example.org')
        .post(urlPath)
        .reply(500)

      let store = new WebStore()

      let dataset = rdf.dataset(simpleGraph, rdf.namedNode('http://example.org' + urlPath))

      return expectError(() => {
        return rdf.waitFor(store.import(dataset.toStream()))
      })
    })
  })

  describe('.remove', () => {
    it('should fetch and replace', () => {
      const urlPath = '/remove-fetch-replace'

      const originalDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 1')
        ),
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 2')
        )
      ])

      const removeDataset = rdf.dataset(
        originalDataset.match(null, null, rdf.literal('object 1')),
        rdf.namedNode('http://example.org' + urlPath)
      )

      const expectedDataset = originalDataset.match(null, null, rdf.literal('object 2'))

      let sentDataset

      nock('http://example.org')
        .get(urlPath)
        .reply(200, originalDataset.toString(), {'content-type': 'text/turtle'})

      nock('http://example.org')
        .put(urlPath)
        .reply(200, function (url, body) {
          let mediaType = this.req.headers['content-type'][0]

          rdf.dataset().import(formats.parsers.import(mediaType, stringToStream(body))).then((dataset) => {
            sentDataset = dataset
          })

          return [simpleGraphNT, {'Content-Type': 'text/turtle'}]
        })

      let store = new WebStore()

      return rdf.waitFor(store.remove(removeDataset.toStream())).then(() => {
        assert(expectedDataset.equals(sentDataset))
      })
    })

    it('should do nothing if stream contains no quads', () => {
      const removeDataset = rdf.dataset()

      let store = new WebStore()

      return rdf.waitFor(store.remove(removeDataset.toStream()))
    })

    it('should not send replace if no quads have been removed', () => {
      const urlPath = '/remove-no-changes'

      const originalDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 1')
        )
      ])

      const removeDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 2'),
          rdf.namedNode('http://example.org' + urlPath)
        )
      ])

      nock('http://example.org')
        .get(urlPath)
        .reply(200, originalDataset.toString(), {'content-type': 'text/turtle'})

      let store = new WebStore()

      return rdf.waitFor(store.remove(removeDataset.toStream()))
    })

    it('should handle fetch errors', () => {
      const urlPath = '/remove-fetch-error'

      const removeDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 1'),
          rdf.namedNode('http://example.org' + urlPath)
        )
      ])

      nock('http://example.org')
        .get(urlPath)
        .reply(500)

      nock('http://example.org')
        .put(urlPath)
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return expectError(() => {
        return rdf.waitFor(store.remove(removeDataset.toStream()))
      })
    })

    it('should handle replace errors', () => {
      const urlPath = '/remove-replace-error'

      const removeDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object'),
          rdf.namedNode('http://example.org' + urlPath)
        )
      ])

      nock('http://example.org')
        .get(urlPath)
        .reply(200, simpleGraphNT, {'content-type': 'text/turtle'})

      nock('http://example.org')
        .put(urlPath)
        .reply(500)

      let store = new WebStore()

      return expectError(() => {
        return rdf.waitFor(store.remove(removeDataset.toStream()))
      })
    })
  })

  describe('.removeMatches', () => {
    it('should fetch and replace', () => {
      const urlPath = '/remove-matches-fetch-replace'

      const originalDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 1')
        ),
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 2')
        )
      ])

      const expectedDataset = originalDataset.match(null, null, rdf.literal('object 2'))

      let sentDataset

      nock('http://example.org')
        .get(urlPath)
        .reply(200, originalDataset.toString(), {'content-type': 'text/turtle'})

      nock('http://example.org')
        .put(urlPath)
        .reply(200, function (url, body) {
          let mediaType = this.req.headers['content-type'][0]

          rdf.dataset().import(formats.parsers.import(mediaType, stringToStream(body))).then((dataset) => {
            sentDataset = dataset
          })

          return [simpleGraphNT, {'Content-Type': 'text/turtle'}]
        })

      let store = new WebStore()

      return rdf.waitFor(store.removeMatches(null, null, rdf.literal('object 1'), rdf.namedNode('http://example.org' + urlPath))).then(() => {
        assert(expectedDataset.equals(sentDataset))
      })
    })

    it('should not send replace if no quads have been removed', () => {
      const urlPath = '/remove-matches-no-changes'

      const originalDataset = rdf.dataset([
        rdf.quad(
          rdf.namedNode('http://example.org/subject'),
          rdf.namedNode('http://example.org/predicate'),
          rdf.literal('object 1')
        )
      ])

      nock('http://example.org')
        .get(urlPath)
        .reply(200, originalDataset.toString(), {'content-type': 'text/turtle'})

      let store = new WebStore()

      return rdf.waitFor(store.removeMatches(null, null, rdf.literal('object 2'), rdf.namedNode('http://example.org' + urlPath)))
    })

    it('should handle fetch errors', () => {
      const urlPath = '/remove-matches-fetch-error'

      nock('http://example.org')
        .get(urlPath)
        .reply(500)

      nock('http://example.org')
        .put(urlPath)
        .reply(200, simpleGraphNT, {'Content-Type': 'text/turtle'})

      let store = new WebStore()

      return expectError(() => {
        return rdf.waitFor(store.removeMatches(null, null, rdf.literal('object 1'), rdf.namedNode('http://example.org' + urlPath)))
      })
    })

    it('should handle replace errors', () => {
      const urlPath = '/remove-matches-replace-error'

      nock('http://example.org')
        .get(urlPath)
        .reply(200, simpleGraphNT, {'content-type': 'text/turtle'})

      nock('http://example.org')
        .put(urlPath)
        .reply(500)

      let store = new WebStore()

      return expectError(() => {
        return rdf.waitFor(store.removeMatches(null, null, rdf.literal('object'), rdf.namedNode('http://example.org' + urlPath)))
      })
    })
  })

  describe('.deleteGraph', function () {
    it('should use http DELETE method', () => {
      const urlPath = '/delete-method'

      nock('http://example.org')
        .delete(urlPath)
        .reply(201)

      let store = new WebStore()

      return rdf.waitFor(store.deleteGraph(rdf.namedNode('http://example.org' + urlPath)))
    })

    it('should handle request error', () => {
      const urlPath = '/delete-graph-client-error'

      const options = {
        fetch: () => {
          return Promise.reject(new Error('error'))
        }
      }

      let store = new WebStore(options)

      return expectError(() => {
        return rdf.waitFor(store.deleteGraph(rdf.namedNode('http://example.org' + urlPath)))
      })
    })

    it('should handle error status code', () => {
      const urlPath = '/delete-graph-status-error'

      nock('http://example.org')
        .delete(urlPath)
        .reply(500)

      let store = new WebStore()

      return expectError(() => {
        return rdf.waitFor(store.deleteGraph(rdf.namedNode('http://example.org' + urlPath)))
      })
    })
  })
})
