import React, {useEffect, useState} from 'react';
import {Button, Card, Form, Input, message, Select,} from 'antd';
import {addUser, getDataNodeList, getUser, modifyUser} from "../../../services/service";

function UserEdit(props) {

    const [form] = Form.useForm();
    const {Option} = Select;

    const onFinish = (values) => {
        let data = {
            username: values.username,
            secret: values.secret,
            storageInfo: {
                dataNodes: values.dataNodes ? values.dataNodes : []
            }
        }
        if (props.match.params.username) {
            modifyUser(data)
                .then(res => {
                    if (res.code !== 0) {
                        message.error(res.msg);
                        return;
                    }
                    message.success("更新用户成功")
                    props.history.push("/admin/users")
                })
        } else {
            addUser(data)
                .then(res => {
                    if (res.code !== 0) {
                        message.error(res.msg);
                        return;
                    }
                    message.success("新增用户成功")
                    props.history.push("/admin/users")
                })
        }
    }
    const [user, setUser] = useState({});
    const [datanodes, setDatanodes] = useState([]);


    useEffect(() => {
            if (props.match.params.username) {
                getUser(props.match.params.username)
                    .then(res => {
                        setUser(res.data)
                        form.resetFields();
                    })
            }
            getDataNodeList()
                .then(res => {
                    setDatanodes(res.data)
                })
        }, []);


    return (
        <Card title={props.match.params.username ? "更新用户" : "新增用户"} bordered
              extra={
                  <Button type={"primary"} size={"large"} onClick={() => props.history.push("/admin/users")}>
                      返回
                  </Button>
              }
        >
            <Form labelCol={{span: 4}}
                  wrapperCol={{span: 14}}
                  name="user"
                  onFinish={onFinish}
                  form={form}>
                <Form.Item
                    label="用户名"
                    name="username"
                    rules={[
                        {
                            required: true,
                            message: '用户名称不能为空',
                        },
                    ]}
                    initialValue={user.username}
                >
                    <Input style={{width: 200}} placeholder="请输入用户名称"/>
                </Form.Item>

                <Form.Item
                    label="用户秘钥"
                    name="secret"
                    rules={[
                        {
                            required: true,
                            message: '用户秘钥不能为空',
                        },
                    ]}
                    initialValue={user.secret}
                >
                    <Input style={{width: 200}} placeholder="请输入用户秘钥"/>
                </Form.Item>
                <Form.Item label="数据节点配额"
                           name="dataNodes"
                >
                    <Select
                        mode="multiple"
                        allowClear
                        defaultValue={user.storageInfo ? user.storageInfo.dataNodes : []}
                        style={{width: '100%'}}
                        placeholder="选择DataNode节点"
                    >
                        {
                            datanodes.map(e => {
                                return (
                                    <Option key={e.hostname} value={e.hostname}>{e.hostname}</Option>
                                );
                            })}
                    </Select>
                </Form.Item>
                <Form.Item wrapperCol={{offset: 8, span: 16}}>
                    <Button type="primary" htmlType="submit">
                        确定
                    </Button>
                </Form.Item>
            </Form>
        </Card>
    )
}

export default UserEdit;