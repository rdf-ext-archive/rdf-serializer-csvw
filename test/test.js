/* global describe, it */

const assert = require('assert')
const concatStream = require('concat-stream')
const fs = require('fs')
const path = require('path')
const rdf = require('rdf-ext')
const sinkTest = require('rdf-sink/test')
const CSVSerializer = require('..')
const Readable = require('readable-stream')

function concat (input) {
  return new Promise((resolve) => {
    input.pipe(concatStream(resolve))
  })
}

describe('rdf-serializer-csvw', () => {
  sinkTest(CSVSerializer, {readable: true})

  it('should serialize incoming quads with IRI column names if no metadata is given', () => {
    const quad0 = rdf.quad(
      rdf.namedNode('http://example.org/subject'),
      rdf.namedNode('http://example.org/property0'),
      rdf.literal('value0')
    )
    const quad1 = rdf.quad(
      rdf.namedNode('http://example.org/subject'),
      rdf.namedNode('http://example.org/property1'),
      rdf.literal('value1')
    )

    const csv = 'http://example.org/property0,http://example.org/property1,subject\nvalue0,value1,http://example.org/subject\n'

    const input = new Readable({
      objectMode: true,
      read: () => {
        input.push(quad0)
        input.push(quad1)
        input.push(null)
      }
    })

    const serializer = new CSVSerializer()
    const stream = serializer.import(input)

    return concat(stream).then((result) => {
      assert.equal(result.toString(), csv)
    })
  })

  it('should serialize incoming quads with column names from metadata', () => {
    const quad0 = rdf.quad(
      rdf.namedNode('http://example.org/subject'),
      rdf.namedNode('http://example.org/property0'),
      rdf.literal('value0')
    )
    const quad1 = rdf.quad(
      rdf.namedNode('http://example.org/subject'),
      rdf.namedNode('http://example.org/property1'),
      rdf.literal('value1')
    )

    const csv = 'property0,property1,subject\nvalue0,value1,http://example.org/subject\n'

    const input = new Readable({
      objectMode: true,
      read: () => {
        input.push(quad0)
        input.push(quad1)
        input.push(null)
      }
    })

    const serializer = new CSVSerializer({
      metadata: JSON.parse(fs.readFileSync(path.join(__dirname, 'support/metadata.json')))
    })
    const stream = serializer.import(input)

    return concat(stream).then((result) => {
      assert.equal(result.toString(), csv)
    })
  })
})
