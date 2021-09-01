import React, {useState} from 'react';
import {Layout, Menu, Avatar, Dropdown, message} from 'antd';
import {adminRoutes} from "../../routes";
import {withRouter} from 'react-router-dom';
import {CaretDownOutlined} from '@ant-design/icons'
import './Index.css'
import {clearToken} from "../../utils/auth";


const {Header, Content, Sider} = Layout;


const routes = adminRoutes.filter(route => route.isShow);


function Index(props) {

    const popMenu = (
        <Menu onClick={(p) => {
            if (p.key === 'logout') {
                clearToken()
                props.history.push("/login")
            } else {
                message.info("暂未实现")
            }
        }}>
            <Menu.Item key="notify">通知中心</Menu.Item>
            <Menu.Item key="setting">设置</Menu.Item>
            <Menu.Item key="logout">退出</Menu.Item>
        </Menu>
    );

    const [currentKey, setCurrentKey] = useState("/admin/dashboard");

    const onMenuSelect = (value) => {
        setCurrentKey(value.key)
    }

    return (
        <Layout>
            <Header className="header">
                <div className="title">
                    儒猿-石杉架构-自研分布式文件系统
                </div>
                <Dropdown className="dropdown" overlay={popMenu}>
                    <div>
                        <Avatar className="avatar" size="small">U</Avatar>
                        <span>超级管理员</span>
                        <CaretDownOutlined/>
                    </div>
                </Dropdown>
            </Header>
            <Layout>
                <Sider width={200} className="site-layout-background">
                    <Menu
                        mode="inline"
                        defaultSelectedKeys={[props.location.pathname]}
                        selectedKeys={[currentKey]}
                        defaultOpenKeys={['sub1']}
                        onSelect={onMenuSelect}
                        style={{height: '100%', borderRight: 0}}
                    >
                        {routes.map((route, index) => {
                            return (
                                <Menu.Item key={route.path} onClick={(p) => {
                                    props.history.push(p.key)
                                }} icon={route.icon}>
                                    {route.title}
                                </Menu.Item>
                            )
                        })}
                    </Menu>
                </Sider>
                <Layout style={{padding: '16px'}}>
                    {/*<Breadcrumb style={{margin: '16px 0'}}>*/}
                    {/*    <Breadcrumb.Item>Home</Breadcrumb.Item>*/}
                    {/*    <Breadcrumb.Item>List</Breadcrumb.Item>*/}
                    {/*    <Breadcrumb.Item>App</Breadcrumb.Item>*/}
                    {/*</Breadcrumb>*/}
                    <Content
                        className="site-layout-background"
                        style={{
                            padding: 24,
                            margin: 0,
                            minHeight: 240,
                            height: '100%',
                            background: "white",
                            overflow: "auto"
                        }}
                    >
                        {props.children}
                    </Content>
                </Layout>
            </Layout>
        </Layout>
    );
}

export default withRouter(Index);