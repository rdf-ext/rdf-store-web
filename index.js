import rdf from 'rdf-ext'
import FilterStream from 'rdf-stream-filter'
import TripleToQuadTransform from 'rdf-transform-triple-to-quad'
import promiseToEvent from 'rdf-utils-stream/promiseToEvent.js'
import chunks from 'stream-chunks/chunks.js'
import handleResponse from './lib/handleResponse.js'

class WebStore {
  constructor ({ factory = rdf } = {}) {
    this.factory = factory
  }

  async _deleteGraph (graph) {
    const res = await this.factory.fetch(graph.value, { method: 'DELETE' })

    await handleResponse(res)
  }

  async _importGraph (graph, dataset, { truncate } = {}) {
    const stream = new TripleToQuadTransform(graph, { factory: this.factory })
    dataset.toStream().pipe(stream)

    const res = await this.factory.fetch(graph.value, {
      method: truncate ? 'PUT' : 'POST',
      body: stream
    })

    const quadStream = await handleResponse(res)

    if (quadStream) {
      await chunks(quadStream)
    }
  }

  async _import (stream, options) {
    const dataset = await rdf.dataset().import(stream)

    if (dataset.size === 0) {
      return
    }

    const graph = [...dataset][0]?.graph

    await this._importGraph(graph, dataset, options)
  }

  async _remove (stream) {
    const remove = await rdf.dataset().import(stream)

    // do nothing if there are no quads
    if (remove.size === 0) {
      return
    }

    const graph = [...remove][0]?.graph

    const existing = await rdf.dataset().import(this.match(null, null, null, graph))
    const updated = existing.difference(remove)

    // don't update if there are no changes
    if (updated.size === existing.size) {
      return
    }

    return this._importGraph(graph, updated, { truncate: true })
  }

  async _removeMatches (subject, predicate, object, graph) {
    const existing = await rdf.dataset().import(this.match(null, null, null, graph))
    const remove = existing.match(subject, predicate, object)

    // don't update if there are no changes
    if (remove.size === 0) {
      return
    }

    const updated = existing.difference(remove)

    await this._importGraph(graph, updated, { truncate: true })
  }

  match (subject, predicate, object, graph) {
    const stream = new TripleToQuadTransform(graph, { factory: this.factory })

    Promise.resolve().then(async () => {
      try {
        const res = await this.factory.fetch(graph.value)
        const quadStream = await handleResponse(res)
        const filteredStream = new FilterStream(quadStream, subject, predicate, object)

        filteredStream.pipe(stream)
      } catch (err) {
        stream.destroy(err)
      }
    })

    return stream
  }

  import (stream, options) {
    return promiseToEvent(this._import(stream, options))
  }

  remove (stream) {
    return promiseToEvent(this._remove(stream))
  }

  removeMatches (subject, predicate, object, graph) {
    return promiseToEvent(this._removeMatches(subject, predicate, object, graph))
  }

  deleteGraph (graph) {
    return promiseToEvent(this._deleteGraph(graph))
  }
}

export default WebStore
