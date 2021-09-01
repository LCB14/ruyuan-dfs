export function getToken() {
    return localStorage.getItem("token");
}

export function setToken(token) {
    return localStorage.setItem("token", token);
}

export function clearToken() {
    return localStorage.removeItem("token");
}

export function isLogined() {
    if (getToken()) {
        return true;
    }
    return false;
}