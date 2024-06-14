async function handleResponse (res) {
  if (res.status > 299) {
    throw new Error('http error')
  }

  if (typeof res.quadStream === 'function') {
    return res.quadStream()
  }

  return null
}

export default handleResponse
