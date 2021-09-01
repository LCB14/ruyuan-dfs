import Login from "../pages/Login";
import PageNotFound from "../pages/404";
import {DashboardOutlined, UserOutlined} from '@ant-design/icons';
import Dashboard from "../pages/admin/dashboard/Dashboard";
import UserList from "../pages/admin/users/UserList";
import UserEdit from "../pages/admin/users/UserEdit";
import UserFiles from "../pages/admin/users/UserFiles";

export const mainRoutes = [
    {
        path: '/login',
        component: Login
    },
    {
        path: '/404',
        component: PageNotFound
    }
];

export const adminRoutes = [
    {
        path: '/admin/dashboard',
        component: Dashboard,
        isShow: true,
        title: "首页",
        icon: <DashboardOutlined/>
    },
    {
        path: '/admin/users',
        component: UserList,
        isShow: true,
        exact: true,
        title: "用户列表",
        icon: <UserOutlined/>
    },
    {
        path: '/admin/users/add/:username?',
        component: UserEdit,
        isShow: false,
        title: "新增用户",
        icon: <UserOutlined/>
    },
    {
        path: '/admin/users/files/:username',
        component: UserFiles,
        isShow: false,
        title: '用户文件列表',
        icon: <UserOutlined/>
    },
    {
        path: '/404',
        component: PageNotFound,
        exact: true
    }];