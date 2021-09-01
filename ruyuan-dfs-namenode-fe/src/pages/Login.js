import React from 'react';
import {Form, Input, Button, Checkbox, Card, message} from 'antd';
import {UserOutlined, LockOutlined} from '@ant-design/icons';
import './login.css'
import {setToken} from "../utils/auth";
import {login} from "../services/service";

function Login(props) {

    const onFinish = (values) => {
        console.log('Received values of form: ', values);
        login({
            username: values.username,
            password: values.password
        }).then(res => {
            console.log(res)
            if (res.code === 0) {
                message.success("登陆成功");
                setToken(values.username);
                props.history.push("/admin")
            } else {
                message.error(res.msg)
            }
        }).catch(err => {
            console.log(err)
        })
    };
    document.title = "登陆";
    return (
        <Card title="儒猿分布式文件系统" className="login-card">
            <Form
                name="normal_login"
                className="login-form"
                onFinish={onFinish}
            >
                <Form.Item
                    initialValue="admin"
                    name="username"
                    rules={[{required: true, message: '请输入用户名!'}]}
                >
                    <Input
                        size="large"
                        prefix={<UserOutlined className="site-form-item-icon"/>}
                        placeholder="用户名"
                        // defaultValue="admin"
                    />
                </Form.Item>
                <Form.Item
                    initialValue="admin"
                    name="password"
                    rules={[{required: true, message: '请输入密码!'}]}
                >
                    <Input.Password
                        size="large"
                        prefix={<LockOutlined className="site-form-item-icon"/>}
                        type="password"
                        placeholder="密码"
                        // defaultValue="admin"
                    />
                </Form.Item>
                <Form.Item>
                    <Form.Item name="remember" valuePropName="checked" noStyle>
                        <Checkbox>记住我</Checkbox>
                    </Form.Item>
                </Form.Item>

                <Form.Item>
                    <Button type="primary" htmlType="submit" className="login-form-button">
                        登陆
                    </Button>
                </Form.Item>
            </Form>
        </Card>
    )
}

export default Login;