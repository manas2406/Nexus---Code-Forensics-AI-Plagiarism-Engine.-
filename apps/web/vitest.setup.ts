import '@testing-library/jest-dom'
class ResizeObserverMock {
  observe() {}
  unobserve() {}
  disconnect() {}
}
global.ResizeObserver = ResizeObserverMock as any;
window.ResizeObserver = ResizeObserverMock as any;
class DOMMatrixReadOnly {
  m11() {} m12() {} m13() {} m14() {}
  m21() {} m22() {} m23() {} m24() {}
  m31() {} m32() {} m33() {} m34() {}
  m41() {} m42() {} m43() {} m44() {}
}
global.DOMMatrixReadOnly = DOMMatrixReadOnly as any;
window.DOMMatrixReadOnly = DOMMatrixReadOnly as any;
global.DOMMatrix = DOMMatrixReadOnly as any;
window.DOMMatrix = DOMMatrixReadOnly as any;
