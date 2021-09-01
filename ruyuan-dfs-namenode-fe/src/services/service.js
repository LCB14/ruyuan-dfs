import {get, post, del, put} from "../utils/request";

export function login(user) {
    return get("/api/admin/login", user)
}

export function getUserList() {
    return get("/api/user/")
}

export function getUser(username) {
    return get("/api/user/" + username)
}

export function getDataNodeList() {
    return get("/api/nodes/datanodes")
}

export function addUser(param) {
    return post("/api/user/", param)
}

export function deleteUser(username) {
    return del("/api/user/" + username)
}

export function modifyUser(param) {
    return put("/api/user/", param)
}

export function getNameNodeList() {
    return get("/api/nodes/namenodes")
}

export function listUserFile(param) {
    return get("/api/user/listFiles", param)
}

export function moveToTrash(param) {
    return put("/api/user/moveToTrash", param)
}

export function getDataNodeByFilename(param) {
    return get("/api/nodes/getFileStorageInfo", param)
}

export function recoverFromTrash(param) {
    return put("/api/user/trash/resume", param)
}

export function changeReplicaNum(param) {
    return get("/api/nodes/changeReplicaNum", param)
}