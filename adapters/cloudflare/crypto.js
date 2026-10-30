// Synchronous, local FFI only. Native Python keeps its existing cryptography pin.
import { ed25519 } from '@noble/curves/ed25519.js';
import { bytesToHex, hexToBytes } from '@noble/curves/utils.js';

function bytes(value, length) {
  if (typeof value !== 'string' || !/^(?:[0-9a-f]{2})*$/.test(value)) {
    throw new TypeError('Expected lowercase hexadecimal bytes');
  }
  const result = hexToBytes(value);
  if (length !== undefined && result.length !== length) throw new RangeError('Invalid byte length');
  return result;
}

export function publicKey(seedHex) {
  return bytesToHex(ed25519.getPublicKey(bytes(seedHex, 32)));
}

export function sign(seedHex, messageHex) {
  return bytesToHex(ed25519.sign(bytes(messageHex), bytes(seedHex, 32)));
}

export function verify(publicHex, messageHex, signatureHex) {
  try {
    return ed25519.verify(bytes(signatureHex, 64), bytes(messageHex), bytes(publicHex, 32), {
      zip215: false,
    });
  } catch {
    return false;
  }
}
