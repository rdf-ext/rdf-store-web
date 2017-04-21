const rdf = require('rdf-ext')
const rdfFetch = require('rdf-fetch')
const FilterStream = require('rdf-stream-filter')
const TripleToQuadTransform = require('rdf-transform-triple-to-quad')

class Store {
  constructor (options) {
    options = options || {}

    this.factory = options.factory || rdf
    this.fetch = options.fetch || rdfFetch
  }

  match (subject, predicate, object, graph) {
    let stream = new TripleToQuadTransform(graph, {factory: this.factory})

    this.fetch(graph.value).then((res) => {
      return Store.handleResponse(res, stream).then((quadStream) => {
        return new FilterStream(quadStream, subject, predicate, object).pipe(stream)
      })
    }).catch((err) => {
      stream.emit('error', err)
    })

    return stream
  }

  importGraph (iri, graph, options) {
    options = options || {}

    const method = options.truncate ? 'put' : 'post'

    return this.fetch(iri.value || iri, {
      method: method,
      body: graph.toStream()
    }).then((res) => {
      return Store.handleResponse(res)
    }).then((quadStream) => {
      if (quadStream) {
        quadStream.resume()

        return rdf.waitFor(quadStream)
      }
    })
  }

  import (stream, options) {
    options = options || {}

    return rdf.asEvent(() => {
      return rdf.dataset().import(stream).then((dataset) => {
        if (dataset.length === 0) {
          return
        }

        const iri = dataset.toArray().shift().graph.value

        return this.importGraph(iri, dataset, options)
      })
    })
  }

  remove (stream) {
    return rdf.asEvent(() => {
      return rdf.dataset().import(stream).then((remove) => {
        // do nothing if there are no quads
        if (remove.length === 0) {
          return
        }

        const iri = remove.toArray().shift().graph

        return rdf.dataset().import(this.match(null, null, null, iri)).then((existing) => {
          const updated = existing.difference(remove)

          // don't update if there are no changes
          if (updated.length === existing.length) {
            return
          }

          return this.importGraph(iri, updated, {truncate: true})
        })
      })
    })
  }

  removeMatches (subject, predicate, object, graph) {
    return rdf.asEvent(() => {
      return rdf.dataset().import(this.match(null, null, null, graph)).then((existing) => {
        const remove = existing.match(subject, predicate, object)

        // don't update if there are no changes
        if (remove.length === 0) {
          return
        }

        const updated = existing.difference(remove)

        return this.importGraph(graph, updated, {truncate: true})
      })
    })
  }

  deleteGraph (graph) {
    return rdf.asEvent(() => {
      return this.fetch(graph.value, {method: 'delete'}).then((res) => {
        return Store.handleResponse(res)
      })
    })
  }

  static handleResponse (res) {
    if (res.status > 299) {
      return Promise.reject(new Error('http error'))
    }

    return res.quadStream()
  }
}

module.exports = Store
