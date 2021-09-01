import axios from 'axios'
import {getToken} from "./auth";

const instance = axios.create({
    baseURL: "http://localhost:8081",
    timeout: 5000
});

export function get(url, params) {
    return instance.get(url, {
        params
    });
}

export function post(url, data) {
    return instance.post(url, data);
}

export function put(url, data) {
    return instance.put(url, data);
}

export function del(url) {
    return instance.delete(url);
}

// 全局请求拦截
instance.interceptors.request.use(function (config) {
    config.headers['authorization'] = 'Bearer ' + getToken()
    return config;
}, function (error) {
    return Promise.reject(error)
});

instance.interceptors.response.use(function (response) {
    return response.data;
});